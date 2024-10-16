#ifndef BIGQ_H
#define BIGQ_H

#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <vector>
#include <pthread.h>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <algorithm>
#include "ComparisonEngine.h"
#include <thread>

class BigQ {
private:
    Pipe &in;              // Reference to the input pipe
    Pipe &out;             // Reference to the output pipe
    OrderMaker &sortOrder;        // Reference to the order maker for sorting
    int runLength;                // The run length in pages
	std::thread worker;

	//void worker();
	int sort(Pipe &in);

public:
    // Constructor: Initializes the BigQ with input and output pipes, sorting order, and run length
    BigQ(Pipe &inPipe, Pipe &outPipe, OrderMaker &sortOrder, int runLen);

    // Destructor: Joins the worker thread and closes the file
    ~BigQ();
};

#endif // BIGQ_H

#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
    int sort(Pipe &in);
    worker();
};

#endif
