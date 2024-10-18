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
    // Create thread
    //cout << "Create worker thread\n";
    thread worker(&BigQ::sortWorker, this);
    //cout << "Worker thread created\n";

    if (worker.joinable()) {
        std::cout << "Worker thread initialized successfully\n";
    }

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
    // finally shut down the out pipe
    worker.detach();
}

void BigQ::sortWorker() {
    cout << "Worker thread is running...\n";
    Record* record = new Record();
    std::vector<Record*> records;
    std::vector<off_t> runLocation;
    ComparisonEngine compare;
    Page page;
    int recordCap = runLength * PAGE_SIZE;
    File file;
    char* fileName = "records.txt";
    file.Open(0, fileName);

    // Loop while there are still records in the pipe
    while (this->in.Remove(record) != 0) {
        runLocation.push_back(file.GetLength());
        for (int numRecords = 0; numRecords < recordCap; numRecords++) {
            records.push_back(record);
            numRecords++;
            if (this->in.Remove(record) == 0) {
                break;
            }
            //cout << "Grabbing Record.\n";
        }
        //sort vector
        std::sort(records.begin(), records.end(), [&](Record* left, Record* right) {
            return compare.Compare(left, right, &sortOrder) < 0;
        });
        // Push vector to file
        for (Record* record : records) {
            Record* temp = new Record();                    ///////////////////
            temp->Copy(record);                             // Need to remove somehow... Deep copy is 
            //cout << "Next record: " << &temp << endl;     // expensive and unessesarry
                                                            ///////////////////
            // If page is full push it to the file.
            if (page.Append(temp) == 0) {
                cout << "Next page\n";
                file.AddPage(&page, file.GetLength());
                page.EmptyItOut();
            }
            free(temp);
        }
        file.AddPage(&page, file.GetLength());
        page.EmptyItOut();
        records.clear();
    }

    // TODO PHOEBE ////////////////////////////////TODO PHOEBE//////////////////////////////
    // Implement part 2 of TPMMS
    // load the first element of each run and push the smallest to out
    // replace the pushed element with the next smallest and repeat
    // until all elements are pushed to out
    // The start of each run is stored in off_t runLocation

    Page* page_ptr;
    //for (off_t run : runLocation) {
        file.GetPage(page_ptr, runLocation.front());
        page_ptr->MoveToFirst();
        while (page_ptr->GetFirst(record) != 0) {
            out.Insert(record);
        }
        //cout << "Inserted record in output pipe\n";
    //}
    cout << "Worker Done" << endl;
    out.ShutDown();
    
}
// hello world

BigQ::~BigQ () {
    std::cout << "BigQ destructor called" << std::endl;
    if (worker.joinable()) {  // Check if the worker thread is still running
        worker.join();        // Wait for the worker thread to finish
    }
}

int BigQ::sort (std::vector<Record*> &records){
	return 0;
}
/*
// Worker function to be run by the worker threadl
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