import sys
import time
import ray
""" Run this script locally to execute a Ray program on your Ray cluster on
Kubernetes.

Before running this script, you must port-forward from the local host to
the relevant Kubernetes head service e.g.
kubectl -n ray port-forward service/long-short-test 10001:10001.

Set the constant LOCAL_PORT below to the local port being forwarded.
"""
LOCAL_PORT = 10001

def isPrime(n):
    if n > 1:
        # check for factors
        for i in range(2, n):
            if (n % i) == 0:
                # print(n, "is not a prime number")
                # print(i, "times", n//i, "is", n)
                # sys.stdout.flush()
                return False
        
        # print(n, "is a prime number")
        # sys.stdout.flush()
        return True
    else:
        return False

"""
    Count the primes from 0 to maxNumber
"""
@ray.remote
def primeCount(maxNumber):
    start = time.time()

    if maxNumber < 0:
        return 0
    
    primeCount = 0
    for n in range(2, maxNumber):
        if isPrime(n):
            primeCount += 1
            # print("Found a prime. Current prime count:", primeCount)
            # sys.stdout.flush()

    return primeCount, time.time() - start 

def wait_for_nodes(expected):
    # Wait for all nodes to join the cluster.
    while True:
        resources = ray.cluster_resources()
        node_keys = [key for key in resources if "node" in key]
        num_nodes = sum(resources[node_key] for node_key in node_keys)
        if num_nodes < expected:
            print("{} nodes have joined so far, waiting for {} more.".format(
                num_nodes, expected - num_nodes))
            sys.stdout.flush()
            time.sleep(1)
        else:
            break


def main():
    wait_for_nodes(3)

    if len(sys.argv) < 2:
        print("Expected integer to represent max number to check for primes. Optionally specify difFactor.")
        print("Short = maxNumber/difFactor")
        print(f"Usage {sys.argv[0]} maxNumber [difFactor]")
        return 1

    maxNumber = int(sys.argv[1])
    difFactor = 10
    if len(sys.argv) < 3:
        difFactor = float(sys.argv[2])

    start = time.time()
    pcLongOutRef = primeCount.remote(maxNumber)
    pcShortOutRef = primeCount.remote(int(maxNumber/difFactor))

    print("Number of primes Short:", ray.get(pcShortOutRef))
    print("Number of primes Long:", ray.get(pcLongOutRef))
    print("Total time =", time.time() - start)

    print("Success!")

if __name__ == "__main__":
    ray.init(f"ray://127.0.0.1:{LOCAL_PORT}")
    main()