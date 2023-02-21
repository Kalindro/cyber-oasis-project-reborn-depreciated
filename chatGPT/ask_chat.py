from dataclasses import dataclass

from API.chatGPT_initiator import initiate_chatGPT_wrapper, initiate_chatGPT_API


@dataclass
class ChatGPTDialog:
    API_mode = 1

    def main(self, question: str) -> str:
        response = self._ask_question_API(question) if self.API_mode == 1 else self._ask_question_wrapper(question)

        return response

    @staticmethod
    def _ask_question_wrapper(question: str) -> str:
        chatbot = initiate_chatGPT_wrapper()
        print("chatGPT:")
        for data in chatbot.ask(question):
            response = data["message"]

        return response

    @staticmethod
    def _ask_question_API(question: str) -> str:
        openai = initiate_chatGPT_API()
        print("chatGPT:")
        response = openai.Completion.create(model="text-davinci-003", prompt=question, temperature=0, max_tokens=60,
                                            top_p=1.0, frequency_penalty=0.5, presence_penalty=0.0)

        return response["choices"][0]["text"].lstrip()


if __name__ == "__main__":
    tweet = "Visitors Can Sell Bitcoin in Dubai for Cash in 2023 at SBID Crypto OTC - NFC"
    full_question = f"Decide if this is positive, neutral or negative news. Tweet: {tweet}"
    ChatGPTDialog().main(question=full_question)
