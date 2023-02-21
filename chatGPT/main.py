from API.chatGPT_initiator import initiate_chatGPT_wrapper, initiate_chatGPT_API
from dataclasses import dataclass


@dataclass
class ChatGPTDialog:
    API_mode = 1

    def ask_question_wrapper(self, question: str) -> str:
        chatbot = initiate_chatGPT_wrapper()
        print("Asked question")
        print("chatGPT:")
        print(chatbot.ask(question))

        for data in chatbot.ask(question):
            print(data)
            response = data["message"]

        return response

    def ask_question_API(self, question: str) -> str:
        openai = initiate_chatGPT_API()
        print("Asked question")
        print("chatGPT:")
        response = openai.Completion.create(model="text-davinci-003", prompt=question, temperature=0, max_tokens=60,
                                            top_p=1.0, frequency_penalty=0.5, presence_penalty=0.0)

        return response["choices"][0]["text"].lstrip()

    def main(self, question: str) -> str:
        response = self.ask_question_API(question) if self.API_mode == 1 else self.ask_question_wrapper(question)

        print(response)
        return response


if __name__ == "__main__":
    tweet = "Visitors Can Sell Bitcoin in Dubai for Cash in 2023 at SBID Crypto OTC - NFC"
    full_question = f"Decide if this is positive, neutral or negative news. Tweet: {tweet}"
    ChatGPTDialog().main(question=full_question)
