from PIL import Image
import fitz
import numpy as np

def summary_paragraph(client, paragraph):
    system_prompt = '''
            Bạn là chuyên giao trong việc tóm tắt ngắn gọn các văn bản lịch sử.
            Hãy tóm tắt ngắn gọn đoạn văn được cung cấp nhưng tuyệt đối không được làm mất đi các thông tin lịch sử quan trọng.
            '''
    response = client.chat.completions.create(
            model='gpt-4o-mini',
            temperature=0,

        messages=[
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": paragraph
            }
            ]
        )
    return response.choices[0].message.content

def extract_key_word(client, summary):
    system_prompt = '''
              Bạn là chuyên gia trong việc trích xuất từ khóa cho thông tin lịch sử.
              Hãy tìm ra một từ/cụm từ khóa có thể thể hiện tổng quát nội dung cốt lõi của đoạn văn.
              YÊU CẦU:
              Chỉ cung cấp từ khóa, không đưa thông tin gì thêm.
              '''
    response = client.chat.completions.create(
            model='gpt-4o-mini',
            temperature=0,

        messages=[
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": summary
            }
            ]
        )
    return response.choices[0].message.content

def pdf_to_images(documents):
    """
    Chuyển đổi từng trang của file PDF sang định dạng PIL Image.
    Args:
        documents (fitz.Document): Đối tượng PDF.
    Returns:
        list: Một list các dictionary, mỗi dict chứa 'image' (PIL Image)
              và 'page_number' của trang tương ứng.
    """

    doc_images = []
    for page_index, page in enumerate(documents):
        pix = page.get_pixmap(dpi=300)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        doc_images.append({
            "image": img,
            "page_index": page_index,
            "page": page
        })
        print(f"  - Đã chuyển đổi trang {page_index}")

    return doc_images

def detect_layout(model_detect_layout, pil_image_obj):
    """
    Phát hiện bố cục trên một PIL Image bằng model YOLOv10.
    Args:
        model_detect_layout: Model Doclayout-yolo
        pil_image_obj (PIL.Image.Image): Đối tượng PIL Image của trang.
    Returns:
        ultralytics.engine.results.Results: Đối tượng kết quả từ YOLOv10 predict.
    """

    results = model_detect_layout.predict(
                  pil_image_obj,   # Image to predict
                  imgsz=1024,        # Prediction image size
                  conf=0.3,          # Confidence threshold
                  device="cpu"    # Device to use (e.g., 'cuda:0' or 'cpu')
              )
    return results[0]

def sort_bboxes_top_to_bottom_left_to_right(boxes, row_tolerance=15):
    """
    Sắp xếp các bounding box theo thứ tự từ trên xuống dưới, từ trái sang phải.
    Thuật toán tối ưu: O(n log n) thay vì O(n²)

    Args:
        boxes: List các box từ YOLO results
        row_tolerance: Ngưỡng để coi các box cùng hàng (pixels)

    Returns:
        List các box đã được sắp xếp
    """
    if not boxes:
        return []

    # Phương pháp 1: Sắp xếp đơn giản - NHANH NHẤT cho hầu hết trường hợp
    # O(n log n) với constant factor thấp
    return sorted(boxes, key=lambda box: (
        int(box.xyxy[0][1].item()),  # y1 (top) - ưu tiên cao
        int(box.xyxy[0][0].item())   # x1 (left) - ưu tiên thấp
    ))

def recognize_text_from_image(reader, img_array_or_pil_image):
    """
    Thực hiện OCR trên một hình ảnh (NumPy array hoặc PIL Image) bằng EasyOCR.
    Args:
        reader: easyocr.readers.ImageReader
        img_array_or_pil_image: Hình ảnh dưới dạng NumPy array (OpenCV format) hoặc PIL Image.
    Returns:
        list: Một list các tuple (bbox, text, confidence) từ EasyOCR.
    """
    results = reader.readtext(img_array_or_pil_image, detail=0)
    return results

def recognize_text_from_pymupdf_page(docs, page_index, bbox):
    """
    Trích xuất văn bản từ một trang PyMuPDF trong một vùng (bounding box) nhất định.

    Args:
        docs (fitz.Document): Đối tượng PDF.
        page_index (int): Index của trang trong PDF.
        bbox (list hoặc tuple): Bounding box dưới dạng [x1, y1, x2, y2], đây là tọa độ hình ảnh

    Returns:
        str: Văn bản được trích xuất từ vùng đã cho. Trả về chuỗi rỗng nếu không tìm thấy text.
    """
    label_2 = ''
    try:

        # chuyển sang tọa độ hình ảnh sang tọa độ PDF
        # 300 DPI = 300/72 = 4.167 pixels per point

        scale = 300/72
        # Tạo một fitz.Rect từ bounding box
        x1, y1, x2, y2 = [coord / scale for coord in bbox]

        # Tạo clip rect và trích xuất text
        clip_rect = fitz.Rect(x1, y1, x2, y2)
        pymupdf_page = docs[page_index]
        block = pymupdf_page.get_text('dict',clip= clip_rect)
        for spans in block['blocks']:
          for line in spans['lines']:
              font = line['spans'][0]['font']
              if 'BoldMT' in font or 'BoldItalicMT' in font or 'ItalicMT' in font:
                  label_2 = 'title'
        block = pymupdf_page.get_text('blocks', clip = clip_rect)
        text = block[0][4]
        text = text.replace('.\n','.#')
        text = text.replace('\n',' ')

        return label_2, text

    except Exception as e:
        print(f"  ❌ Lỗi khi trích xuất text từ PyMuPDF: {str(e)}")
        return '', "" # Trả về chuỗi rỗng nếu có lỗi


def process_pdf_page(docs, model_detect_layout, pdf_page_data, continue_index):
    """
    Xử lý một trang PDF: phát hiện bố cục và nhận dạng văn bản.
    Args:
        model_detect_layout: model Doclayout_yolo
        pdf_page_data (dict): Dictionary chứa 'image' (PIL Image) và 'page_index'.
        continue_index (int): Index tiếp tục từ lần xử lý trước
    Returns:
        tuple: (continue_index, processed_paragraphs, page_results)
    """
    page_index = pdf_page_data["page_index"]
    pil_image = pdf_page_data["image"]

    print(f"\n--- Xử lý trang: {page_index} ---")

    # 1. Phát hiện bố cục
    layout_results = detect_layout(pil_image)
    processed_paragraphs = []

    print("\n  >>> Kết quả phát hiện bố cục:")

    # Kiểm tra xem có boxes không
    if not (hasattr(layout_results, 'boxes') and layout_results.boxes):
        print("    Không tìm thấy đối tượng bố cục nào.")
        return continue_index, processed_paragraphs
    # 2. Sắp xếp các bounding box theo thứ tự đọc tự nhiên
    sorted_boxes = sort_bboxes_top_to_bottom_left_to_right(layout_results.boxes)

    print(f"    Tìm thấy {len(sorted_boxes)} đối tượng bố cục")
    lable_2 = ''
    # 3. Xử lý từng box theo thứ tự đã sắp xếp
    for i, box in enumerate(sorted_boxes):
        bbox = box.xyxy[0].tolist()
        x1, y1, x2, y2 = map(int, bbox)
        label = model_detect_layout.names[int(box.cls[0])]
        score = box.conf[0].item()
        # Chỉ xử lý box không phải abandon
        if label == 'abandon':
            print(f"      Bỏ qua do là chú thích")
            continue
        # Chỉ xử lý box có confidence >= threshold
        if score < 0.3:
            print(f"      Bỏ qua do confidence thấp ({score:.3f} < 0.3)")
            continue


        print(f"    Box {i+1}: {label} (confidence: {score:.3f}) at [{x1}, {y1}, {x2}, {y2}]")
        continue_index += 1

        try:
            # Cắt ảnh theo bbox
            image_cut = pil_image.crop((x1, y1, x2, y2))
            img_np = np.array(image_cut)

            # 4. Nhận dạng văn bản
            # recognized_text_results = recognize_text_from_image(img_np)
            lable_from_pymupdf, recognized_text_results = recognize_text_from_pymupdf_page(docs, page_index, bbox)

            # 5. Tạo thông tin paragraph
            if recognized_text_results:
                    final_label = label
                    if lable_from_pymupdf == 'title':
                        final_label = 'title'
                    paragraph_info = {
                        'type': final_label,
                        'full_text': recognized_text_results,
                        'page_index': page_index,
                        'parent_index': -1,
                        'index': continue_index,
                        'is_title': label == 'title'
                    }

                    processed_paragraphs.append(paragraph_info)

            else:
                print(f"      ⚠ Không nhận dạng được text")
                continue_index -= 1  # Rollback index nếu không nhận dạng được

        except Exception as e:
            print(f"      ❌ Lỗi khi xử lý box: {str(e)}")
            continue_index -= 1  # Rollback index nếu có lỗi

    print(f"\n  >>> Hoàn thành xử lý trang {page_index}: {len(processed_paragraphs)} paragraphs")

    return continue_index, processed_paragraphs


def process_full_pdf(pdf_path):
    """
    Xử lý toàn bộ file PDF: chuyển đổi, phát hiện bố cục và nhận dạng văn bản từng trang.
    Args:
        pdf_path (str): Đường dẫn đến file PDF.
    Returns:
        dict: Dictionary chứa tất cả kết quả xử lý và thống kê
    """
    print(f"\n🚀 Bắt đầu xử lý PDF: {pdf_path}")
    documents = fitz.open(pdf_path)
    # Chuyển đổi PDF thành ảnh
    all_page_images = pdf_to_images(documents)
    total_pages = len(all_page_images)
    print(f"📄 Tổng số trang: {total_pages}")

    # Khởi tạo kết quả

    all_paragraphs = []
    continue_index = 0

    # Xử lý từng trang
    for i, page_data in enumerate(all_page_images, 1):
        print(f"\n📖 Đang xử lý trang {i}/{total_pages}...")

        try:
            # Xử lý trang và nhận kết quả
            continue_index, page_paragraphs = process_pdf_page(documents, page_data, continue_index)

            # Thêm paragraphs vào danh sách tổng
            all_paragraphs.extend(page_paragraphs)

            print(f"✅ Hoàn thành trang {i}: {len(page_paragraphs)} paragraphs")

        except Exception as e:
            print(f"❌ Lỗi khi xử lý trang {i}: {str(e)}")


    # Tạo thống kê tổng quan
    total_paragraphs = len(all_paragraphs)


    return {
        "pdf_path": pdf_path,
        "total_pages": total_pages,
        "total_paragraphs": total_paragraphs,
        "all_paragraphs": all_paragraphs,

    }
