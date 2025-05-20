from flask import Flask, request, jsonify
from flask_cors import CORS
from LLMquery import *
import json
import time
from random import sample
app = Flask(__name__)
CORS(app)

chat_histories = {}
user_id = '234'
name_ontology = 'MINDMAP'
@app.route("/chat", methods=["POST"])
def chat():
    if user_id not in chat_histories:
        chat_histories[user_id] = []

    data = request.json
    question = data.get("message", "")
    
    start_time = time.time()
    relation = find_relation(onto)
    print("\n====================== ENTITIES ==============================")
    entities_with_annotation_sumarry = get_entities_with_annotation(onto, 'summary')
    explication = create_explication(entities_with_annotation_sumarry)
    entities = find_entities_from_question_PP1(relation,explication, question, chat_histories[user_id])

    list_query = create_query(name_ontology, json.loads(entities) )
    print("list_query: ", list_query)

    print("\n====================== KẾT QUẢ TRA CỨU =======================\n")
    result_from_ontology = find_question_info(name_ontology, list_query)
    print("result_from_ontology: ", result_from_ontology)
    raw_informations_from_ontology = []
    try: 
        for result in result_from_ontology[0]:
                raw_informations_from_ontology.append(result)
    except:
        raw_informations_from_ontology.append("Không có thông tin cho câu hỏi")

    k_similar_info = find_similar_info_from_raw_informations(question, raw_informations_from_ontology)
    print("\n====================== SIMILAR INFO ===========================\n")
    print(k_similar_info)
    bot_response = generate_response(relation , k_similar_info, question, chat_histories[user_id])
    end_time = time.time()
    print("Thời gian thực thi:", end_time - start_time, "giây")

    chat_histories[user_id].append({"sender": "user", "text": question})
    chat_histories[user_id].append({"sender": "bot", "text": bot_response})

    return jsonify({"response": bot_response
                    })

if __name__ == "__main__":
    app.run(debug=True)
