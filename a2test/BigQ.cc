#include "BigQ.h"
#include <thread>
#include <queue>
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
    //std::vector<off_t> runLocation;
    ComparisonEngine compare;
    int recordCap = runLength * PAGE_SIZE / sizeof(record);
    File file;
    char* fileName = "records.txt";
    file.Open(0, fileName);
    int numPages = 0;
    int numRuns = 0;
    Record* temp = new Record();
    Record* toAdd = new Record();

    // Loop while there are still records in the pipe
    while (this->in.Remove(record) != 0) {
        //runLocation.push_back(file.GetLength());
        numRuns++;
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
                numPages++;
                page.EmptyItOut();
                page.Append(temp);
            }
            //cout << "Appended Record to page\n";
            free(temp);
        }
        if (!page.IsEmpty()) {
            file.AddPage(&page, file.GetLength());
            cout << "Added page to file\n";
            cout << "There are " << page.GetNumRecs() << " records on the page\n";
            numPages++;
        }
        page.EmptyItOut();
        records.clear();
    }

    
    // TODO PHOEBE ////////////////////////////////TODO PHOEBE//////////////////////////////
    // Implement part 2 of TPMMS
    // load the first element of each run and push the smallest to out
    // replace the pushed element with the next smallest and repeat
    // until all elements are pushed to out
    // The start of each run can be found with run# * runlength.
    // If you run out of pages in a run, ignore this run in future checks.
    runSecondPhaseTPMMS(out, sortOrder, runLength, file, numRuns);
    
    cout << "Worker Done" << endl;
    file.Close();
    out.ShutDown();
    
}
// hello world

// Custom comparison for the priority queue (min-heap)
struct CompareRecords {
    OrderMaker &order;        // Reference to the OrderMaker
    ComparisonEngine &compare;  // Reference to the ComparisonEngine

    // Constructor to initialize OrderMaker and ComparisonEngine
    CompareRecords(OrderMaker &orderMaker, ComparisonEngine &compEngine) 
        : order(orderMaker), compare(compEngine) {}

    // Overload the call operator to perform the comparison
    bool operator()(const std::pair<Record*, int>& left, const std::pair<Record*, int>& right) {
        // Use compare.Compare to compare the two Record pointers based on the OrderMaker
        return compare.Compare(left.first, right.first, &order) > 0;
    }
};

void BigQ::runSecondPhaseTPMMS(Pipe& outputPipe, OrderMaker& sortOrder, int runLength, File& file, int numRuns) {
    // Priority queue to store the smallest records from each page (min-heap)
    ComparisonEngine compare;
    std::priority_queue<std::pair<Record*, int>, std::vector<std::pair<Record*, int>>, CompareRecords> minHeap(CompareRecords(sortOrder, compare));

    // Record to hold the data extracted from the pages
    Record* toAdd = new Record();
    std::vector<int> pageOffset(numRuns, 0);  // Track the current page within each run
    std::vector<Page*> pageArray(numRuns, nullptr);  // Page pointers for each run
    int totalPages = file.GetLength();

    // Initialize the heap with the first record from each run
    for (int run = 0; run < numRuns; ++run) {
        int pageNumber = run * runLength;  // The first page of each run
        file.GetPage(pageArray[run], pageNumber);
        if (pageArray[run]->GetFirst(toAdd) != 0) {  // If the page has at least one record
            minHeap.push({new Record(*toAdd), run});  // Push the first record and its corresponding run index
        }
    }

    // Merge process
    while (!minHeap.empty()) {
        // Get the smallest record from the heap
        auto minRec = minHeap.top();
        minHeap.pop();

        // Output the smallest record to the final sorted output
        outputPipe.Insert(minRec.first);

        // Get the next record from the same run
        int runIndex = minRec.second;
        bool recordAvailable = false;

        // Try to fetch the next record from the current page in the run
        if (pageArray[runIndex]->GetFirst(toAdd) != 0) {
            // Still records in the current page, add the next record to the heap
            minHeap.push({new Record(*toAdd), runIndex});
            recordAvailable = true;
        }

        // If no record available in the current page, move to the next page within the same run
        if (!recordAvailable) {
            pageOffset[runIndex]++;  // Move to the next page in the run
            int pageNumber = runIndex * runLength + pageOffset[runIndex];  // Calculate the page number

            if (pageOffset[runIndex] < runLength && pageNumber < totalPages) {
                // If there are still pages left in this run
                file.GetPage(pageArray[runIndex], pageNumber);  // Load the next page in the run
                if (pageArray[runIndex]->GetFirst(toAdd) != 0) {  // Fetch the first record from the new page
                    minHeap.push({new Record(*toAdd), runIndex});
                }
            }
        }

        // Free the memory for the extracted record
        delete minRec.first;
    }

    // Clean up the allocated memory
    delete toAdd;
}

BigQ::~BigQ () {
    std::cout << "BigQ destructor called" << std::endl;
    if (worker.joinable()) {  // Check if the worker thread is still running
        worker.join();        // Wait for the worker thread to finish
        cout << "Worker joined\n";
    }
}

int BigQ::sort (std::vector<Record*> &records){
	return 0;
}