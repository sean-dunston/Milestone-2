#include "BigQ.h"
#include <thread>
using namespace std;

//BigQ Constructor
//  create 1 worker thread
//      Constantly call inputPipe.remove to get all records
//          Sort with TPMMS
//              Accept run length pages of records
//              Sort these records
//                  Use the OrderMaker class to determine sorted order.
//              Write out sorted records as a run
//              Append all runs to the same file
//  return from constructor
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortOrder, int runlen) : 
    in(in), out(out), runLength(runlen), sortOrder(sortOrder) {
    cout << "\nBigQ initialized\n";
    cout << "SortOrder: " << sortOrder.getNumAtts() << endl;
    //Open file to store pages
    //file.Open(0, "sorted.txt");
    int numPages = 0;
    // Create thread
    thread worker(&BigQ::sortWorker, this);

    //worker.join();
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	// out.ShutDown ();
}

void BigQ::sortWorker() {
    cout << "Worker thread is running...\n";
    Record* record = new Record();
    std::vector<Record*> records;
    int recordCap = runLength * PAGE_SIZE;

    // Loop while there are still records in the pipe
    cout << "Loop\n";
    while (this->in.Remove(record) != 0) {
        records.push_back(record);
        cout << "Grabbing Record.\n";
    }

    ComparisonEngine compare;
    cout << "Created Comparator\n";

    // Sort the records using the Compare function
    std::sort(records.begin(), records.end(), [&](Record* left, Record* right) {
        return compare.Compare(left, right, &sortOrder) < 0;
    });

    out.Insert(records.front());
    cout << "Worker Done" << endl;
}

BigQ::~BigQ () {
    //file.Close();
}

int BigQ::sort (Pipe &in){
	return 0;
}
/*
// Worker function to be run by the worker thread
void BigQ::worker() {
    cout << "Try to do work" << endl;
    Page records;
    Record *temp;
    in.Remove(temp);
    records.Append(temp);
    cout << "Worker Started" << endl;
    return;
}
//Pipe: Insert, Remove

//


/*
#include "BigQ.h"

// Worker function to be run by the worker thread
void* BigQ::worker(void* arg) {
    cout << "Try to do work" << endl;
    BigQ* bigQ = static_cast<BigQ*>(arg);
    bigQ->sortAndProcess();
    cout << "Worker Started" << endl;
    return nullptr;
}

// Function to process records from inputPipe, sort them into runs, and write to file
void BigQ::sortAndProcess() {
    std::vector<Record> records;
    Record temp;

    int pageCount = 0;

    // Loop through the input pipe, creating sorted runs and writing them to the file
    cout << "Remove from pipe" << endl;
    while (inputPipe.Remove(&temp)) {
        records.push_back(temp);
        cout << "Pushed a record to the vector" << endl;
        // Simulate the page size check. If the records exceed the run length (number of pages), sort and write to file
        pageCount++;  // Assume each record takes up one page for simplicity

        if (pageCount >= runLength) {
            sortRecords(records);
            cout << "Sorted Records" << endl;
            writeRunToFile(records);
            cout << "Wrote to File" << endl;
            records.clear();  // Clear buffer for the next run
            pageCount = 0;    // Reset page count for the next run
        }
        cout << "SortAndProcess Looped once" << endl;
    }
    cout << "Removed from pipe" << endl;
    // Process any remaining records
    if (!records.empty()) {
        sortRecords(records);
        writeRunToFile(records);
    }

    outputPipe.ShutDown();  // Signal that we're done producing
}

// Function to sort records according to the OrderMaker
void BigQ::sortRecords(std::vector<Record>& records) {
    std::sort(records.begin(), records.end(), [&](const Record &a, const Record &b) {
        return compare.Compare(const_cast<Record*>(&a), const_cast<Record*>(&b), &sortOrder) < 0;
    });
}

// Function to write a sorted run to the file
void BigQ::writeRunToFile(std::vector<Record>& records) {
    Page page;
    page.EmptyItOut();  // Start with an empty page

    for (Record& rec : records) {
        if (!page.Append(&rec)) {  // If the page is full, write the page to the file and start a new page
            file.AddPage(&page, currentPageOffset);
            currentPageOffset++;   // Increment the offset for the next page
            page.EmptyItOut();
            page.Append(&rec);     // Append the record to the new page
        }
    }

    // Write the last page
    if (page.GetNumRecs() > 0) {
        file.AddPage(&page, currentPageOffset);
        currentPageOffset++;   // Increment the offset for the next page
    }
}

// Constructor: Initializes the BigQ with input and output pipes, sorting order, and run length
BigQ::BigQ(Pipe &inPipe, Pipe &outPipe, OrderMaker &sortOrder, int runLen)
    : inputPipe(inPipe), outputPipe(outPipe), sortOrder(sortOrder), runLength(runLen), file() {
    // Open file to store sorted runs
    cout << "try to open" << endl;
    file.Open(0, "sorted_runs.bin");
    cout << "It Opened" << endl;

    // Initialize the offset for page writing
    currentPageOffset = 0;

    // Create a worker thread to process records
    pthread_create(&workerThread, nullptr, worker, this);
    cout << "Thread Created" << endl;
}

// Destructor: Joins the worker thread and closes the file
BigQ::~BigQ() {
    // Wait for the worker thread to finish
    pthread_join(workerThread, nullptr);

    // Close file after processing
    file.Close();
}
*/