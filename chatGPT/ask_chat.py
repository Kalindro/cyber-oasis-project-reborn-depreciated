from API.chatGPT_initiator import initiate_chatGPT_wrapper, initiate_chatGPT_API
from utils.utils import clean_string_from_spaces_and_links


def ask_question_wrapper(question: str) -> str:
    chatbot = initiate_chatGPT_wrapper()
    for data in chatbot.ask(question):
        response = data["message"]

        return response


def ask_question_API(question: str) -> str:
    openai = initiate_chatGPT_API()
    response = openai.Completion.create(model="text-davinci-003", prompt=question, temperature=0, max_tokens=60,
                                        top_p=1.0, frequency_penalty=0.5, presence_penalty=0.0)

    return clean_string_from_spaces_and_links(response["choices"][0]["text"])
