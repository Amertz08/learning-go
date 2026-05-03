# learning-go

Captures examples of my focused learning in Go. A lot of this code is from assorted books, and exercises from early in the
books may not use the best patterns which are introduced in later chapters. I say that to state that I would not take these
examples as the pinnacle of how I can write Go code. I would instead look to the following examples that I implemented that
better exemplify the patterns and idioms that I learned later in my journey with Go.

- [Pipeline Capstone](concurrency/pipeline_cap/main.go)
- [Data Structures](datastructures/)
- [Remote Shell](networking/reverse_shell)


## AI Stuff

Writing the code by hand to further ingrain knowledge, but I have used ChatGPT to generate the interfaces for data structures.

### Potential workflows

1. Write Ginkgo specs in a `test-spec.txt` file
2. Have an Agent generate test suite via spec file
3. Validate the correctness of the test suite
4. Have the Agent generate the implementation (disallow any changes to the test code)

In the case of an API
1. Write Ginkgo specs in a `test-spec.txt` file
2. Have an Agent generate an OpenAPI spec
   1. Make any changes necessary
3. Have the Agent generate the test suite with the test and OpenAPI spec 
4. Validate the correctness of the test suite
5. Have the agent generate the implementation (disallow any changes to the test code)
