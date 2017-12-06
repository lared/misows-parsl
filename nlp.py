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

# done synchronously because it only eats memory
# for now done in phases to test the algo
corpusText = "abc         \n                def" #loadCorpus()

step = divideIntoParagraphs(corpusText)
paragraphs = step.result()

norm_futures = [normalizeWhitespace(paragraph) for paragraph in paragraphs]
normalized_paragraphs = [future.result() for future in norm_futures]

print(normalized_paragraphs)
