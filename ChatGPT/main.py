from API.chatGPT_initiator import initiate_chatGPT_wrapper, initiate_chatGPT_API


class ChatGPTDialog:

    def __init__(self):
        self.openai = initiate_chatGPT_API()
        self.chatbot = initiate_chatGPT_wrapper()

    def ask_question_wrapper(self, question):
        prev_text = ""
        print("Asked question")
        print("ChatGPT:")
        for data in self.chatbot.ask(question):
            message = data["message"][len(prev_text):]
            print(message, end="", flush=True)
            prev_text = data["message"]
        print()

    def ask_question_API(self, question):
        print("Asked question")
        print("ChatGPT:")
        response = self.openai.Completion.create(model="text-davinci-003", prompt=question, temperature=0,
                                                 max_tokens=60, top_p=1.0, frequency_penalty=0.5, presence_penalty=0.0)
        print(response["choices"][0]["text"])


if __name__ == "__main__":
    tweet = "Sram sobie"
    question = f"Decide if a Tweet's sentiment is positive, neutral, or negative and how impactful. /Tweet: {tweet}"
    ChatGPTDialog().ask_question_API(question)
