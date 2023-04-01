import os

import openai
from dotenv import load_dotenv
from revChatGPT.V1 import Chatbot

load_dotenv()


def initiate_chatGPT_wrapper():
    email = os.getenv("CHATGPT_EMAIL")
    password = os.getenv("CHATGPT_PASSWORD")
    chatbot = Chatbot(config={"email": email, "password": password})

    return chatbot


def initiate_chatGPT_API():
    API_key = os.getenv("CHATGPT_API_KEY")
    openai.api_key = API_key

    return openai
