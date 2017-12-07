import os
import re
import parsl
from pathlib import Path
from parsl import *

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(workers)

INPUTS_DIR = 'input'

def loadCorpus():
    filesToRead = ["{}/{}".format(INPUTS_DIR, filename) \
            for filename in os.listdir(INPUTS_DIR)]

    fullText = ""

    for fileName in filesToRead:
        fullText += Path(fileName).read_text()

    return fullText

def isBlank(line):
    return not bool(line.strip())

@App('python', dfk)
def divideIntoParagraphs(text):
    paragraphs = []
    partialParagraph = []

    lines = text.splitlines()

    for line in lines:
        if isBlank(line):
            if len(partialParagraph) > 0:
                completeParagraph = " ".join(partialParagraph)
                partialParagraph = []
                paragraphs.append(completeParagraph)
        else:
            partialParagraph.append(line)

    paragraphs.append(" ".join(partialParagraph))
    return [paragraph for paragraph in paragraphs if not isBlank(paragraph)]

@App('python', dfk)
def normalizeWhitespace(paragraph):
    return re.sub("\s+", " " , paragraph)

@App('python', dfk)
def removeSpecialCharsExceptDots(paragraph):
    return re.sub("[^a-zA-Z0-9\. ]+", "" , paragraph)

@App('python', dfk)
def divideIntoSentences(paragraph):
    return [sentence.strip() for sentence in paragraph.split(".") if not isBlank(sentence)]

@App('python', dfk)
def calculateKMostCommonNGramsInefficiently(text, n, k):
    frequency = dict()
    for i in range(0, len(text)-2):
        trigram = text[i:i+3]
        frequency[trigram] = frequency.get(trigram, 0) + 1

    sortedMap = sorted(frequency.items(), key=lambda kv: (kv[1],kv[0]))
    sortedMap = sortedMap[:10]

    return [value for key, value in sortedMap]


# the actual flow setup
corpusText = "This sentence does not matter. Neither does this one, ending here:.\n\nThis sentence ends in another line." #loadCorpus()

ngramsFuture = calculateKMostCommonNGramsInefficiently(corpusText, 3, 20)

paragraphsFuture = divideIntoParagraphs(corpusText)
# we have to wait - it is not possible to fan-out without that
paragraphs = paragraphsFuture.result()

for paragraph in paragraphs:

    noSpecialsFuture = removeSpecialCharsExceptDots(paragraph)

    whitespaceNormalizedFuture = normalizeWhitespace(noSpecialsFuture)

    sentencesFuture = divideIntoSentences(whitespaceNormalizedFuture)
    sentences = sentencesFuture.result()

    for sentence in sentences:
        #TODO: calculate stuff
        print(">>> " + sentence)
        pass

mostCommonTrigrams = ngramsFuture.result()
# TODO: join the ngrams and paragraph stats
