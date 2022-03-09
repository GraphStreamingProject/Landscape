import random
import string

output = open("/home/hadoop/sample.txt","w")

num_words = 100000000
word_length = 5

letters = string.ascii_lowercase

def generate_word(word_length):
    return ( ''.join(random.choice(letters) for i in range(word_length)) )

for i in range(num_words):
    output.write(generate_word(word_length))
    output.write("\n")

output.close()

