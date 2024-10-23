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
BigQ :: BigQ (Pipe &in, Pipe &out, const OrderMaker &sortOrder, int runlen) : 
    in(in), out(out), runLength(runlen), sortOrder(sortOrder) {
    // Create thread
    //cout << "Create worker thread\n";
    thread worker(&BigQ::sortWorker, this);
    //cout << "Worker thread created\n";

    if (worker.joinable()) {
        //std::cout << "Worker thread initialized successfully\n";
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
    std::vector<int> runStart(100);
    runStart.at(0) = 0;

    char* fileName = "records.txt";
    file.Open(0, fileName);
    int numPages = 0;
    int numRuns = 0;
    int numRecs = 0;

    // Loop while there are still records in the pipe
    bool recordsRemaining = true;
    while (recordsRemaining) {
        int recs = 0;
        bool moreRuns = false;
        while ((recs < 600 * runLength) && recordsRemaining) {
            if (this->in.Remove(record) == 1) {
                recs++;
                numRecs++;
                // Allocate a new record for each record being removed from the pipe
                Record* newRecord = new Record();  // Ensure each record gets its own memory
                newRecord->Copy(record);  // Copy the contents of the current record into the new one
                records.push_back(newRecord);  // Push the unique pointer to the vector
                moreRuns = true;
            } else {
                recordsRemaining = false;
                break;
            }
            
        }
        if (recordsRemaining || moreRuns) numRuns++;
        //std::cout << "Vector Length: " << records.size() << "\tnumRuns: " << numRuns << endl;
        // Sort the records vector
        std::sort(records.begin(), records.end(), [&](Record* left, Record* right) {
            return compare.Compare(left, right, &sortOrder);
        });

        int currentRecords = 0;
        // Push sorted records to the file
        for (Record* tempRecord : records) {
            // Append records to the page
            currentRecords++;
            if (page.Append(tempRecord) == 0) {  // If page is full, push it to the file
                //std::cout << "Next page.  Starting at " << currentRecords << " records.\n";
                file.AddPage(&page, file.GetLength() - 1);
                numPages++;
                //std::cout << "Added page " << numPages << " to file.  " << "There are " << page.GetNumRecs() << " records on the page\n";
                page.EmptyItOut();
                page.Append(tempRecord);  // Append the remaining record to the new page
            }
        }

        if (!page.IsEmpty()) {  // If there's data left in the page, add it to the file
            file.AddPage(&page, file.GetLength() - 1);
            numPages++;
            //std::cout << "Added page " << numPages << " to file.  " << "There are " << page.GetNumRecs() << " records on the page\n";
        }
        runStart.at(numRuns) = file.GetLength();
        // Cleanup and prepare for the next batch of records
        page.EmptyItOut();
        records.clear();  // Clear the vector for the next run
    }

    runSecondPhaseTPMMS(out, sortOrder, runLength, numRuns, runStart);
    //testPhaseOne();
    
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
        return compare.Compare(left.first, right.first, &order);
    }
};

void BigQ::runSecondPhaseTPMMS(Pipe& outputPipe, OrderMaker& sortOrder, int runLength, int numRuns, std::vector<int> runStart) {
    // Priority queue (min-heap) to store the smallest records from each page
    ComparisonEngine compare;
    std::priority_queue<std::pair<Record*, int>, std::vector<std::pair<Record*, int>>, CompareRecords> minHeap(CompareRecords(sortOrder, compare));

    // Record to hold the data extracted from the pages
    Record* record = new Record();
    std::vector<int> pageOffset(numRuns, 0);  // Track the current page within each run
    std::vector<Page*> pageArray(numRuns, nullptr);  // Page pointers for each run
    int totalPages = file.GetLength();  // Get the total number of pages in the file

    //std::cout << "Total pages in the file: " << totalPages << std::endl;

    // Initialize the heap with the first record from each run
    for (int run = 0; run < numRuns; ++run) {
        //int pageNumber = run * runLength;  // Calculate the starting page of each run
        int pageNumber = runStart.at(run);
        //std::cout << "pageNumber: " << pageNumber << " / " << runStart.at(run) << endl;
        //std::cout << "Processing run " << run << " starting from page " << pageNumber << std::endl;

        // Load the first page of the current run
        if (pageNumber < totalPages) {
            if (pageArray[run] == nullptr) {
                pageArray[run] = new Page();  // Allocate a new Page object if needed
            }
            file.GetPage(pageArray[run], pageNumber - 1);  // Load the first page of the current run

            Record* firstRecord = new Record();  // Create a unique pointer for the first record
            if (pageArray[run]->GetFirst(firstRecord) != 0) {  // Get the first record in the page
                minHeap.push({firstRecord, run});  // Push the unique record pointer and its corresponding run index
            } else {
                delete firstRecord;  // If no record is fetched, delete the unused pointer
            }
        }
    }

    // Merge process: extract records from the heap in sorted order and push to the output pipe
    while (!minHeap.empty()) {
        //std::cout << "Processing heap, size: " << minHeap.size() << std::endl;
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

        // If no record is available in the current page, move to the next page within the same run
        if (!recordAvailable) {
            pageOffset[runIndex]++;  // Move to the next page in the run
            int pageNumber = runStart.at(runIndex) + pageOffset[runIndex];  // Calculate the next page number

            if (pageOffset[runIndex] < runLength && pageNumber < totalPages) {
                // Load the next page in the run if there are more pages
                if (pageArray[runIndex] != nullptr) {
                    delete pageArray[runIndex];  // Free the previous Page object
                    pageArray[runIndex] = nullptr;
                }

                pageArray[runIndex] = new Page();  // Allocate a new Page object
                file.GetPage(pageArray[runIndex], pageNumber - 1);  // Load the next page in the run

                Record* nextPageRecord = new Record();  // Create a new unique pointer for the next page
                if (pageArray[runIndex]->GetFirst(nextPageRecord) != 0) {  // Fetch the first record from the new page
                    minHeap.push({nextPageRecord, runIndex});
                } else {
                    delete nextPageRecord;  // If no record is fetched, delete the unused pointer
                }
            }
        }
    }

    // Clean up Page pointers
    for (int run = 0; run < numRuns; ++run) {
        if (pageArray[run] != nullptr) {
            delete pageArray[run];  // Free the Page objects
        }
    }

    // Clean up the dynamically allocated memory
    delete record;

    // Shut down the output pipe to signal that all records have been output
    outputPipe.ShutDown();
}



BigQ::~BigQ () {
    //std::cout << "BigQ destructor called" << std::endl;
    if (worker.joinable()) {  // Check if the worker thread is still running
        worker.join();        // Wait for the worker thread to finish
        cout << "Worker joined\n";
    }
}

int BigQ::sort (std::vector<Record*> &records){
	return 0;
}

void BigQ::testPhaseOne() {
    // Record to hold the data extracted from the pages
    Record* record = new Record();
    Page page;
    int totalPages = file.GetLength();  // Get the total number of pages in the file
    int totalRecords = 0;

    // Iterate through each page in the file
    for (int pageIndex = 0; pageIndex < totalPages; ++pageIndex) {  // Note: file.GetLength() returns pages + 1
        file.GetPage(&page, pageIndex - 1);  // Load the page from the file

        int recordsOnPage = 0;
        // Iterate through all records in the page
        while (page.GetFirst(record) != 0) {
            totalRecords ++; recordsOnPage++;
            out.Insert(record);  // Insert each record into the output pipe
        }
        std::cout << recordsOnPage << " / " << totalRecords << endl;
    }

    // Clean up the allocated memory
    delete record;
}