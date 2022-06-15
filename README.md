# S.A.V.E.
This repo is to validate the accuracy results described in S.A.V.E. thesis.

## Reference
Chang, Y.Y., Wong, S.T., Salawu, E.O., Wang Y.X., Hung, J.H. and Yang,L-W. (2021) [Full-privacy secured search engine empowered by efficient genome-mapping algorithms](https://arxiv.org/abs/2201.00696).

## Requirements

* g++ 10.3.0+
* capacity: >16T
* memory: >120G

## Databases
FM-index: https://drive.google.com/drive/folders/1vN7CzuX2W2AhGyUH0z2GuN7WuDBK91R0?usp=sharing

## Usage
Please download the databases first and make sure your machine meets the requirements.
```cpp
g++ test_collision.cpp -Ofast --std=c++20 -o test_collision -lpthread
./test_collision <output_words> <encoding char number> <comparison number>
```

## Note
The whole process might take several weeks (tested with 32 cores).
