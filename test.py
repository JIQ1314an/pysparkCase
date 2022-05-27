import numpy as np


def partition(A, p, r):
    x = A[r]
    i = p - 1
    for j in range(p, r):
        if A[j] <= x:
            exchange(A, i + 1, j)
            i = i + 1
    exchange(A, i + 1, r)
    q = i + 1
    return q


def exchange(A, i1, i2):
    temp = A[i1]
    A[i1] = A[i2]
    A[i2] = temp


def Rondom(p, r):
    return np.random.randint(p, r + 1)


def Randomized_Partition(A, p, r):
    s = Rondom(p, r)
    exchange(A, s, r)
    q = partition(A, p, r)
    return q


def QuickSort_MinK(A, p, r, k):
    global res
    if p <= r:
        q = Randomized_Partition(A, p, r)
        # print(A, q)
        if q == k-1:
            res = A[q]
            A = [res]

        elif q > k-1:
            A = A[:q]
        else:
            k = k - (q+1)
            A = A[q+1:]

        if len(A) == 1:
            res = A[0]

        p = 0
        r = len(A) - 1
        q = Randomized_Partition(A, p, r)

        QuickSort_MinK(A, p, q - 1, k)
        QuickSort_MinK(A, q + 1, r, k)




if __name__ == "__main__":
    arr = input('数组（元素之间使用“,”隔开）a = ').split(',')
    k = int(input('大于0的整数 k = '))
    A = [int(i.strip()) for i in arr] # [20, 43, 32, 67 ,48, 89, 36, 47, 15]
    QuickSort_MinK(A, 0, len(A) - 1, k)
    print(res)