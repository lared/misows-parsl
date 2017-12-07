import os
import re
from pathlib import Path
from parsl import *
from math import log

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

# count the frequency statistic
@App('python', dfk)
def tf_frequences(paragraphs):
    tf = [None] * len(paragraphs)
    for i in range(len(paragraphs)):
        tf[i] = {}
        for sentence in paragraphs[i]:
            for term in sentence.split():
                if term not in tf[i]:
                    tf[i][term] = 1
                else:
                    tf[i][term] += 1
    return tf

@App('python', dfk)
def df_frequences(tf):
    df = {}
    # df counting, basic on tf
    for term_list in tf:
        for term in term_list:
            if term not in df:
                df[term] = 1
            else:
                df[term] += 1
    return df


@App('python', dfk)
def tf_idf(paragraphs, tf, df):
    weigths = [None] * len(paragraphs)
    for i in range(len(paragraphs)):
        weigths[i] = {}
        for sentence in paragraphs[i]:
            for term in sentence.split():
                weigths[i][term] = tf[i][term]*log(len(paragraphs)/df[term])
        for key, value in weigths[i].items():
            weigths[i][key] = 1.*value/len(weigths[i])
    return weigths


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

    tf = tf_frequences(sentences)
    tf = tf.result()
    print(tf)

    df = df_frequences(tf)
    df = df.result()
    print(df)

    tf_idf_weigths = tf_idf(sentences, tf, df)
    tf_idf_weigths = tf_idf_weigths.result()
    print(tf_idf_weigths)

mostCommonTrigrams = ngramsFuture.result()
# TODO: join the ngrams and paragraph stats
