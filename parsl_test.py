import parsl
from parsl import *

print(parsl.__version__)

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(workers)

@App('python', dfk)
def hello ():
    return 'Hello World!'

app_future = hello()

print("Status: ", app_future.done())

print("Result: ", app_future.result())

@App('python', dfk)
def pi(total):
    # App functions have to import modules they will use.
    import random     
    # Set the size of the box (edge length) in which we drop random points
    edge_length = 10000
    center = edge_length / 2
    c2  = center ** 2
    count = 0
    
    for i in range(total):
        # Drop a random point in the box.
        x,y = random.randint(1, edge_length),random.randint(1, edge_length)
        # Count points within the circle
        if (x-center)**2 + (y-center)**2 < c2:
            count += 1
    
    return (count*4/total)

@App('python', dfk)
def avg_three(a,b,c):
    return (a+b+c)/3

a, b, c = pi(10**6), pi(10**6), pi(10**6)
avg_pi = avg_three(a, b, c)

print("A: {0:.5f} B: {1:.5f} C: {2:.5f}".format(a.result(), b.result(), c.result()))
print("Average: {0:.5f}".format(avg_pi.result()))
