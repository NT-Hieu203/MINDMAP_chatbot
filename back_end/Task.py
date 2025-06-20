from celery import Celery
from flask import Flask
import os
import uuid
import time
from MainProcessor import create_ontology
import redis # Thêm import này
import json

def make_celery(app=None):
    """
    Tạo và cấu hình Celery instance
    """
    # Lấy cấu hình từ environment variables hoặc sử dụng default
    broker_url = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    result_backend = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
    os.environ.setdefault("FORKED_BY_MULTIPROCESSING", "1")
    celery = Celery(
        'tasks',
        broker=broker_url,
        backend=result_backend
    )

    celery.conf.update({
        'broker_url': broker_url,
        'result_backend': result_backend,
        'imports': ['Task'],  # Import module này
        'task_serializer': 'json',
        'accept_content': ['json'],
        'result_serializer': 'json',
        'timezone': 'UTC',
        'enable_utc': True,
        'task_routes': {
            'Task.build_ontology_async_task': {'queue': 'ontology_queue'},
        },
        'worker_prefetch_multiplier': 1,
        'task_acks_late': True,
    })

    # Nếu có Flask app context, tạo ContextTask
    if app:
        class ContextTask(celery.Task):
            def __call__(self, *args, **kwargs):
                with app.app_context():
                    return self.run(*args, **kwargs)

        celery.Task = ContextTask

    return celery


# Khởi tạo Celery instance
celery = make_celery()

# Thư mục để lưu trữ các ontology được tạo
GENERATED_ONTOLOGIES_FOLDER = 'generated_ontologies'
if not os.path.exists(GENERATED_ONTOLOGIES_FOLDER):
    os.makedirs(GENERATED_ONTOLOGIES_FOLDER)

# Global variable để lưu SocketIO instance (sẽ được set từ server.py)
_socketio_instance = None
worker_redis_client = redis.StrictRedis.from_url(os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'))


def set_socketio_instance(socketio_instance):
    """
    Set SocketIO instance để có thể gửi thông báo từ Celery task
    """
    global _socketio_instance
    _socketio_instance = socketio_instance

def get_ontology_state_worker(session_id):
    key = f"ontology_state:{session_id}"
    data = worker_redis_client.get(key)
    return json.loads(data) if data else None

def set_ontology_state_worker(session_id, state_dict):
    key = f"ontology_state:{session_id}"
    worker_redis_client.set(key, json.dumps(state_dict))
    # Tùy chọn: đặt thời gian hết hạn cho key nếu bạn muốn trạng thái tự động bị xóa sau một thời gian
    # worker_redis_client.expire(key, 3600 * 24)

@celery.task(bind=True)
def build_ontology_async_task(self, clustering_tree_data, session_id):
    """
    Tác vụ Celery để xây dựng ontology từ clustering_tree_data.
    Sau khi hoàn thành, gửi thông báo qua WebSocket.

    Args:
        clustering_tree_data: Dữ liệu cây phân cụm để tạo ontology
        session_id: ID của session người dùng

    Returns:
        dict: Trạng thái và thông tin kết quả
    """
    try:
        # Cập nhật trạng thái task
        self.update_state(
            state='BUILDING_ONTOLOGY',
            meta={'message': 'Đang xây dựng Ontology ngầm...'}
        )
        print(f"[Celery Task] Bắt đầu xây dựng Ontology cho session: {session_id}")
        # Tạo IRI duy nhất cho ontology mới để tránh trùng lặp
        new_ontology_iri = f"http://www.semanticweb.org/MINDMAP_CLUSTER_{uuid.uuid4().hex}"

        # Gọi hàm create_ontology từ MainProcessor.py
        new_onto = create_ontology(clustering_tree_data, ontology_iri=new_ontology_iri)

        # Tạo tên file và đường dẫn lưu ontology
        ontology_filename = f"ontology_{session_id}.owl"
        ontology_save_path = os.path.join(GENERATED_ONTOLOGIES_FOLDER, ontology_filename)

        # Lưu Ontology vào file .owl
        new_onto.save(file=ontology_save_path, format="rdfxml")
        print(f"[Celery Task] Ontology đã được lưu tại: {ontology_save_path}")
        current_state = get_ontology_state_worker(session_id)
        if current_state:  # Chỉ cập nhật nếu trạng thái ban đầu được Flask thiết lập
            current_state['status'] = 'completed'
            current_state['ontology_path'] = ontology_save_path
            set_ontology_state_worker(session_id, current_state)
        else:  # Xử lý trường hợp trạng thái có thể chưa được thiết lập (ít xảy ra nếu upload-pdf là đầu tiên)
            set_ontology_state_worker(session_id, {
                'status': 'completed',
                'task_id': self.request.id,
                'ontology_path': ontology_save_path,
                'timestamp': time.time()  # Thêm timestamp nếu chưa có
            })
        # Gửi thông báo qua SocketIO nếu có
        if _socketio_instance:
            try:
                print(f"[Celery Task] Gửi thông báo 'ontology_ready' cho session: {session_id}")
                _socketio_instance.emit('ontology_ready', {
                    'task_id': self.request.id,
                    'status': 'completed',
                    'message': 'Ontology đã sẵn sàng chat!',
                    'ontology_path': ontology_save_path
                }, room=session_id)
            except Exception as socket_error:
                print(f"[Celery Task] Lỗi khi gửi thông báo SocketIO: {socket_error}")

        result = {
            'status': 'completed',
            'ontology_path': ontology_save_path,
            'message': 'Ontology đã được tạo thành công'
        }

        print(f"[Celery Task] Hoàn thành xây dựng Ontology cho session: {session_id}")
        return result

    except Exception as e:
        import traceback
        traceback.print_exc()

        # Cập nhật trạng thái lỗi
        self.update_state(
            state='FAILED',
            meta={
                'exc_type': type(e).__name__,
                'exc_message': str(e)
            }
        )

        print(f"[Celery Task] Lỗi xây dựng Ontology cho session {session_id}: {str(e)}")

        # Cập nhật trạng thái lỗi trong session_ontologies
        current_state = get_ontology_state_worker(session_id)
        if current_state:
            current_state['status'] = 'failed'
            set_ontology_state_worker(session_id, current_state)
        else:  # Hoặc thiết lập nếu không tìm thấy
            set_ontology_state_worker(session_id, {
                'status': 'failed',
                'task_id': self.request.id,
                'error': str(e),
                'timestamp': time.time()
            })
        # Gửi thông báo lỗi qua SocketIO nếu có
        if _socketio_instance:
            try:
                _socketio_instance.emit('ontology_failed', {
                    'task_id': self.request.id,
                    'status': 'failed',
                    'message': f'Lỗi trong quá trình xây dựng Ontology: {str(e)}'
                }, room=session_id)
            except Exception as socket_error:
                print(f"[Celery Task] Lỗi khi gửi thông báo lỗi SocketIO: {socket_error}")

        return {
            'status': 'failed',
            'error': str(e),
            'message': f'Lỗi trong quá trình xây dựng Ontology: {str(e)}'
        }


@celery.task
def test_task(message):
    """
    Task test đơn giản để kiểm tra Celery hoạt động
    """
    print(f"Test task received message: {message}")
    time.sleep(2)  # Simulate some work
    return f"Task completed: {message}"


@celery.task
def cleanup_old_ontologies():
    """
    Task để dọn dẹp các file ontology cũ (có thể chạy định kỳ)
    """
    try:
        import glob
        import time

        # Xóa các file ontology cũ hơn 24 giờ
        current_time = time.time()
        pattern = os.path.join(GENERATED_ONTOLOGIES_FOLDER, "ontology_*.owl")

        deleted_count = 0
        for file_path in glob.glob(pattern):
            file_age = current_time - os.path.getctime(file_path)
            if file_age > 24 * 3600:  # 24 giờ
                try:
                    os.remove(file_path)
                    deleted_count += 1
                    print(f"Đã xóa file ontology cũ: {file_path}")
                except Exception as e:
                    print(f"Lỗi khi xóa file {file_path}: {e}")

        return f"Đã dọn dẹp {deleted_count} file ontology cũ"

    except Exception as e:
        print(f"Lỗi trong quá trình dọn dẹp: {e}")
        return f"Lỗi: {str(e)}"


# Hàm helper để khởi tạo Celery với Flask app context
def init_celery_with_app(app):
    """
    Khởi tạo Celery với Flask app context

    Args:
        app: Flask application instance

    Returns:
        Celery: Configured Celery instance
    """
    return make_celery(app)


# Signal handlers cho Celery
@celery.task(bind=True)
def long_running_task(self, duration=10):
    """
    Task mẫu để test progress tracking
    """
    for i in range(duration):
        time.sleep(1)
        self.update_state(
            state='PROGRESS',
            meta={'current': i + 1, 'total': duration, 'status': f'Processing step {i + 1}'}
        )

    return {'current': duration, 'total': duration, 'status': 'Task completed!', 'result': 42}