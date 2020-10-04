## introduction

Implementation of a Dynamo style key value storage. It implements a simplified version of Dynamo.
Three main features - 

    1. Partitioning
    2. Replication
    3. Faliure Handling

It provides availability and linearizability at the same time, i.e provides read and write operations even with faliures and a read operation always returns the most recent value.

Specification Doc - https://docs.google.com/document/d/1ww5O0ItL0KrDm3HPDLHcJQTkMDyJtdUVSKTZziVkIpk/edit


## running the code

To run the code, you would need Android Studio. We use 5 emulators to simulate a distributed system environment and test the code. 

The logs of the run can be checked in 'Logcat' window of each emulator in Android Studio. 

### setup

#### create and run emulators(AVDs)

`python create_avd.py 5 <path_to_android_sdk>`

`python run_avd.py 5`

### networking

we have fixed sockets and ports. use the following line to enable networking between the emulators. 

`python set_redir.py 10000`

we open one server socket that listens on port 10000. The redirection ports are 11108, 11112, 11116, 11120, and 11124. We hard code these ports.

### running the grader

This project was a part of requirement for CSE 586 at University at Buffalo, therefore a grading script is also available which can be used to test the different features of the code. The script checks for the following 6 phases(it takes about 20 minutes to check everything, as the checks are extensive)

- Testing basic ops
- Testing concurrent ops with different keys
- Testing concurrent ops with same keys
- Testing one failure
- Testing concurrent operations with one failure
- Testing concurrent operations with one consistent failure


`./simpledynamo-grading.linux <path_to_project>/app/build/outputs/apk/debug/app-debug.apk -n 5`

In case of any errors rebuild the apk and re run the command.
