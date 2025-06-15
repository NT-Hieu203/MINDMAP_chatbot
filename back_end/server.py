from flask import Flask, request, jsonify
from flask_cors import CORS
from openai import OpenAI
from owlready2 import *
from dotenv import load_dotenv
from LLMquery import *
import json
import time
import os
from sentence_transformers import SentenceTransformer

app = Flask(__name__)
CORS(app)

load_dotenv(dotenv_path="secrect.env")
OPENAI_API_KEY= os.getenv('OPENAI_API_KEY')
client = OpenAI(api_key = OPENAI_API_KEY)

ONTO_AVAILABLE_PATH = "static/MINDMAP.owl"
name_ontology = ONTO_AVAILABLE_PATH.split('/')[-1].split('.')[0]
ontology_available = get_ontology(ONTO_AVAILABLE_PATH).load()
model_embedding_name = 'paraphrase-multilingual-MiniLM-L12-v2'
model_embedding = SentenceTransformer(model_embedding_name)

chat_histories = {}
user_id = '234'

@app.route("/chat", methods=["POST"])
def chat():
    if user_id not in chat_histories:
        chat_histories[user_id] = []

    data = request.json
    question = data.get("message", "")
    
    start_time = time.time()
    relation = find_relation(ontology_available)
    print("\n====================== ENTITIES ==============================")
    entities_with_annotation_sumarry = get_entities_with_annotation(ontology_available, 'summary')
    explication = create_explication(entities_with_annotation_sumarry)
    entities = find_entities_from_question_PP1(client, relation,explication, question, chat_histories[user_id])

    list_query = create_query(ontology_available, name_ontology, json.loads(entities) )
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

    k_similar_info = find_similar_info_from_raw_informations(model_embedding, question, raw_informations_from_ontology)
    print("\n====================== SIMILAR INFO ===========================\n")
    print(k_similar_info)
    bot_response = generate_response(client , k_similar_info, question, chat_histories[user_id])
    end_time = time.time()
    print("Thời gian thực thi:", end_time - start_time, "giây")

    chat_histories[user_id].append({"sender": "user", "text": question})
    chat_histories[user_id].append({"sender": "bot", "text": bot_response})

    return jsonify({"response": bot_response
                    })

if __name__ == "__main__":
    app.run(debug=True)
