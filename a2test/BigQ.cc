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
    Record* record = new Record();
    std::vector<Record*> records;  // Vector to hold unique record pointers
    ComparisonEngine compare;
    int recordCap = runLength * PAGE_SIZE / sizeof(record);
    char* fileName = "records.txt";
    file.Open(0, fileName);
    int numPages = 0;
    int numRuns = 0;

    // Loop while there are still records in the pipe
    while (this->in.Remove(record) != 0) {
        numRuns++;
        for (int numRecords = 0; numRecords < recordCap; numRecords++) {
            // Allocate a new record for each record being removed from the pipe
            Record* newRecord = new Record();  // Ensure each record gets its own memory
            newRecord->Copy(record);  // Copy the contents of the current record into the new one
            records.push_back(newRecord);  // Push the unique pointer to the vector

            if (this->in.Remove(record) == 0) {
                break;
            }
        }

        // Sort the records vector
        std::sort(records.begin(), records.end(), [&](Record* left, Record* right) {
            return compare.Compare(left, right, &sortOrder) > 0;
        });

        // Push sorted records to the file
        for (Record* record : records) {
            // Append records to the page
            if (page.Append(record) == 0) {  // If page is full, push it to the file
                std::cout << "Next page\n";
                file.AddPage(&page, file.GetLength());
                numPages++;
                page.EmptyItOut();
                page.Append(record);  // Append the remaining record to the new page
            }
        }

        if (!page.IsEmpty()) {  // If there's data left in the page, add it to the file
            file.AddPage(&page, file.GetLength());
            std::cout << "Added page to file\n";
            std::cout << "There are " << page.GetNumRecs() << " records on the page\n";
            numPages++;
        }

        // Cleanup and prepare for the next batch of records
        page.EmptyItOut();
        for (Record* rec : records) {
            //delete rec;  // Free the memory allocated for each unique record
        }
        records.clear();  // Clear the vector for the next run
    }

    runSecondPhaseTPMMS(out, sortOrder, runLength, numRuns);
    
    file.Close();
    out.ShutDown();
    
}
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

void BigQ::runSecondPhaseTPMMS(Pipe& outputPipe, OrderMaker& sortOrder, int runLength, int numRuns) {
    // Priority queue to store the smallest records from each page (min-heap)
    ComparisonEngine compare;
    std::priority_queue<std::pair<Record*, int>, std::vector<std::pair<Record*, int>>, CompareRecords> minHeap(CompareRecords(sortOrder, compare));

    std::vector<int> pageOffset(numRuns, 0);  // Track the current page within each run
    std::vector<Page*> pageArray(numRuns, nullptr);  // Page pointers for each run
    int totalPages = file.GetLength();

    // Initialize the heap with the first record from each run
    for (int run = 0; run < numRuns; ++run) {
        int pageNumber = run * runLength;  // The first page of each run
        if (pageNumber < totalPages - 1) {
            if (pageArray[run] == nullptr) {
                pageArray[run] = new Page();  // Allocate a new Page object
            }
            file.GetPage(pageArray[run], pageNumber);  // Now it's safe to use
            
            Record* firstRecord = new Record();  // Create a unique pointer for the first record
            if (pageArray[run]->GetFirst(firstRecord) != 0) {  // If the page has at least one record
                minHeap.push({firstRecord, run});  // Push the unique record pointer and its corresponding run index
            } else {
                delete firstRecord;  // If no record is fetched, delete the unused pointer
            }
        }
    }

    // Merge process
    while (!minHeap.empty()) {
        // Get the smallest record from the heap
        auto minRec = minHeap.top();
        minHeap.pop();

        // Output the smallest record to the final sorted output
        outputPipe.Insert(minRec.first);

        // No need to delete minRec.first, as the record is consumed by Pipe::Insert

        // Get the next record from the same run
        int runIndex = minRec.second;
        bool recordAvailable = false;

        Record* nextRecord = new Record();  // Create a new unique pointer for the next record
        // Try to fetch the next record from the current page in the run
        if (pageArray[runIndex]->GetFirst(nextRecord) != 0) {
            // Still records in the current page, add the next record pointer to the heap
            minHeap.push({nextRecord, runIndex});
            recordAvailable = true;
        } else {
            delete nextRecord;  // If no record is fetched, delete the unused pointer
        }

        // If no record available in the current page, move to the next page within the same run
        if (!recordAvailable) {
            pageOffset[runIndex]++;  // Move to the next page in the run
            int pageNumber = runIndex * runLength + pageOffset[runIndex];  // Calculate the page number

            if (pageOffset[runIndex] < runLength && pageNumber < totalPages - 1) {
                if (pageArray[runIndex] != nullptr) {
                    delete pageArray[runIndex];  // Free the previous Page object
                    pageArray[runIndex] = nullptr;
                }

                pageArray[runIndex] = new Page();  // Allocate a new Page object
                file.GetPage(pageArray[runIndex], pageNumber);  // Load the next page in the run

                Record* nextPageRecord = new Record();  // Create a new unique pointer for the first record of the new page
                if (pageArray[runIndex]->GetFirst(nextPageRecord) != 0) {  // Fetch the first record from the new page
                    minHeap.push({nextPageRecord, runIndex});
                } else {
                    delete nextPageRecord;  // If no record is fetched, delete the unused pointer
                }
            }
        }

        // No manual deletion of records is done, as they are consumed by Pipe::Insert
    }

    // Clean up Page pointers
    for (int run = 0; run < numRuns; ++run) {
        if (pageArray[run] != nullptr) {
            delete pageArray[run];  // Free the Page objects
        }
    }
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