////////////////////////////////////////////////////////////////////////////////////
//										  //
//  File           : crud_file_io.c						  //
//  Description    : This is the implementation of the standardized IO functions  //
//                   for used to access the CRUD storage system.		  //
//										  //
//  Author         : Jorge Jimenez						  //
//  Last Modified  : Mon Nov 17 12:00:00 EPDT 2014				  //
////////////////////////////////////////////////////////////////////////////////////

// Includes
#include <malloc.h>
#include <string.h>

// Project Includes
#include <crud_file_io.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <crud_network.h>

// Defines
#define CIO_UNIT_TEST_MAX_WRITE_SIZE 1024
#define CRUD_IO_UNIT_TEST_ITERATIONS 10240
#define CRUD_CREATE_PRIORITY 42
#define CRUD_UPDATE_PRIORITY 43
#define CRUD_READ_PRIORITY 44

// Other definitions
char *buffer;

// Type for UNIT test interface
typedef enum {
	CIO_UNIT_TEST_READ   = 0,
	CIO_UNIT_TEST_WRITE  = 1,
	CIO_UNIT_TEST_APPEND = 2,
	CIO_UNIT_TEST_SEEK   = 3,
} CRUD_UNIT_TEST_TYPE;

// File system Static Data
// This the definition of the file table
CrudFileAllocationType crud_file_table[CRUD_MAX_TOTAL_FILES]; // The file handle table

// Pick up these definitions from the unit test of the crud driver
CrudRequest construct_crud_request(CrudOID oid, CRUD_REQUEST_TYPES req,
		uint32_t length, uint8_t flags, uint8_t res);
int deconstruct_crud_request(CrudRequest request, CrudOID *oid,
		CRUD_REQUEST_TYPES *req, uint32_t *length, uint8_t *flags,
		uint8_t *res);

uint64_t check_crud_response;
int CRUD_INITIALIZED = 0;
int RETURN_FAILED = 0;
int fh;
int size = (CRUD_MAX_TOTAL_FILES* sizeof(crud_file_table[0]));
int FLAG;

//
// Implementation
 
void Deconstruct_Crud_response(uint64_t response)
{
	if(response<<63 >> 63 == 0)
	{
                  crud_file_table[fh].length = response <<36 >> 40;
                  crud_file_table[fh].object_id = response >> 32;
        }
 
        else
     	{
        	RETURN_FAILED = 1;
                printf("Result is non 0 \n");
        }
}
void Construct_Crud_Response(int request, void *buf)
{
	uint64_t crud_bus;
	uint64_t response;
	int doDecons = 1;

	switch(request)
	{
		case CRUD_INIT:
		{
			if(CRUD_INITIALIZED == 0)
			{
				crud_bus =  (uint64_t) CRUD_NO_OBJECT << 32;
				crud_bus += (uint64_t)CRUD_INIT << 28;
				crud_bus += (uint64_t) crud_file_table[fh].length << 4;
				crud_bus += (uint64_t) FLAG << 1;
				// return bit
				crud_bus += (uint64_t) 0; 
				CRUD_INITIALIZED = 1;
				crud_file_table[fh].position = 0;
				buffer = malloc(CRUD_MAX_OBJECT_SIZE);
				// Load saved state from crud_content.crd
			}
			
			break;
		}

		case CRUD_CREATE:
		{
			crud_bus =  (uint64_t) CRUD_NO_OBJECT << 32; 
			crud_bus += (uint64_t) CRUD_CREATE << 28;
			crud_bus += (uint64_t) crud_file_table[fh].length << 4;
			crud_bus += (uint64_t) FLAG << 1;
			// return bit
			crud_bus += (uint64_t) 0;
			break;
		}

		case CRUD_READ:
		{
			crud_bus =  (uint64_t) crud_file_table[fh].object_id << 32;
			crud_bus += (uint64_t) CRUD_READ << 28;
			crud_bus += (uint64_t) CRUD_MAX_OBJECT_SIZE << 4;
			crud_bus += (uint64_t) FLAG << 1;
			crud_bus += (uint64_t) 0;
			break;
		}

		case CRUD_DELETE:
		{
			//FLAG = 0;

			crud_bus =  (uint64_t) crud_file_table[fh].object_id << 32;
			crud_bus += (uint64_t) CRUD_DELETE << 28;
			crud_bus += (uint64_t) 0 << 4;
			crud_bus += (uint64_t) 0 << 1;
			crud_bus += (uint64_t) 0;
			//doDecons = 0;
			break;
		}
		
		case CRUD_UPDATE:
		{
			crud_bus = (uint64_t) crud_file_table[fh].object_id << 32;
			crud_bus+= (uint64_t) CRUD_UPDATE << 28;
			crud_bus+= (uint64_t) crud_file_table[fh].length << 4;
			crud_bus+= (uint64_t) FLAG << 1;
			crud_bus+= (uint64_t) 0;
			break;
		}

		case CRUD_FORMAT:
		{
			// Deletes crud_content.crd and all objects in the object store
			// Also wipes priority object

			crud_bus = (uint64_t) 0 << 32;
			crud_bus += (uint64_t) CRUD_FORMAT << 28;
			crud_bus += (uint64_t) 0 << 4;
			crud_bus += (uint64_t) 0 << 1;
			crud_bus += (uint64_t) 0;
			doDecons = 0;
			break;
		}

		case CRUD_CLOSE:
		{
			// Saves the contents of the object store to the crud_content.crd file
			// Creates crud_content.crd if it does not exist
			
			crud_bus = (uint64_t) 0 << 32;
			crud_bus += (uint64_t) CRUD_CLOSE << 28;
			crud_bus += (uint64_t) 0 << 4;
			crud_bus += (uint64_t) 0 << 1;
			crud_bus += (uint64_t) 0;
			break;
		}

		case CRUD_CREATE_PRIORITY:
		{
			// Create for priority object
			
			crud_bus = (uint64_t) 0 << 32;
			crud_bus += (uint64_t) CRUD_CREATE << 28;
			crud_bus += (uint64_t) size << 4;
			crud_bus += (uint64_t) CRUD_PRIORITY_OBJECT << 1;
			crud_bus += (uint64_t) 0;
			doDecons = 0;
			break;
		}

		case CRUD_UPDATE_PRIORITY:
		{
			// Priority object update

			crud_bus = (uint64_t) 0 << 32;
			crud_bus += (uint64_t) CRUD_UPDATE << 28;
			crud_bus += (uint64_t) size<< 4;
			crud_bus += (uint64_t) CRUD_PRIORITY_OBJECT << 1;
			crud_bus += (uint64_t) 0;
			doDecons = 0;
			break;
		}

		case CRUD_READ_PRIORITY:
		{
			// Read for priority object

			crud_bus = (uint64_t) 0 << 32;
			crud_bus += (uint64_t) CRUD_READ << 28;
			crud_bus += (uint64_t) size<< 4;
			crud_bus += (uint64_t) CRUD_PRIORITY_OBJECT << 1;
			crud_bus += (uint64_t) 0;
			doDecons = 0;
			break;
		}


		default:
		{
			printf("\n\n Command not recognized \n\n");
			break;
		}
	}

	response = crud_client_operation(crud_bus, buf);
	if(doDecons) Deconstruct_Crud_response(response);
	check_crud_response = response;
}

//////////////////////////////////////////////////////////////////////////////////
//										//
// Function     : crud_format							//
// Description  : This function formats the crud drive, and adds the file	//
//                allocation table.						//
//										//
// Inputs       : none								//
// Outputs      : 0 if successful, -1 if failure				//
//////////////////////////////////////////////////////////////////////////////////

uint16_t crud_format(void)
{
	// Reinitializes the filesystem and creates an empty file allocation table
	// By performing CRUD_INIT and CRUD_FORMAT
	// Initialize the file allocation table with 0s
	// Save it by creating a priority object containing the table data

	//Initialize variables
	// Iterator for loop
	int iterator;

	// Perform CRUD_INIT and CRUD_FORMAT
	
	// Set CRUD_INITIALIZED to 0 in order to reinitialize it
	CRUD_INITIALIZED = 0;

	if(CRUD_INITIALIZED == 0) Construct_Crud_Response(CRUD_INIT, NULL);

	Construct_Crud_Response(CRUD_FORMAT, NULL);

	for(iterator = 0; iterator < CRUD_MAX_TOTAL_FILES; iterator ++)
	{	
		crud_file_table[iterator].position = 0;	
		crud_file_table[iterator].length = 0;
		strcpy(crud_file_table[iterator].filename, "");
		crud_file_table[iterator].object_id = 0;
		crud_file_table[iterator].open = 0;	
	}

	CrudFileAllocationType *table = malloc(size); 

	memcpy(table, crud_file_table, size);

	Construct_Crud_Response(CRUD_CREATE_PRIORITY, table);

	free(table);
	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... formatting complete.");
	return(0);
}

//////////////////////////////////////////////////////////////////////////////////
//										//
// Function     : crud_mount							//
// Description  : This function mount the current crud file system and loads	//
//                the file allocation table.					//
//										//
// Inputs       : none								//
// Outputs      : 0 if successful, -1 if failure				//
//////////////////////////////////////////////////////////////////////////////////

uint16_t crud_mount(void)
{
	// Loads an existing saved filesystem into the object store
	// and file table by performing a normal CRUD_INIT if it has not already been run.
	// Locate the file allocation table by reading the priority object
	// Copy its contents into the crud_file_table structure

	if(CRUD_INITIALIZED == 0) Construct_Crud_Response(CRUD_INIT, NULL);

	CrudFileAllocationType *table = malloc(size);	
	
	Construct_Crud_Response(CRUD_READ_PRIORITY, table);

	memcpy(crud_file_table, table, size);
	
	free(table);

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... mount complete.");
	return(0);
}

//////////////////////////////////////////////////////////////////////////////////
//										//
// Function     : crud_unmount							//
// Description  : This function unmounts the current crud file system and	//
//                saves the file allocation table.				//
//										//
// Inputs       : none								//
// Outputs      : 0 if successful, -1 if failure				//
//////////////////////////////////////////////////////////////////////////////////

uint16_t crud_unmount(void) 
{
	// Saves all changes to the filesystem and the object store
	// Store the current file back in the storage device by updating the priority object
	// Call CRUD_CLOSE which will write out the persistent state file shutting down vhardware

	if(CRUD_INITIALIZED == 0) Construct_Crud_Response(CRUD_INIT, NULL);

	CrudFileAllocationType *table = malloc(size);

	memcpy(table, crud_file_table, size);

	Construct_Crud_Response(CRUD_UPDATE_PRIORITY, crud_file_table);

	Construct_Crud_Response(CRUD_CLOSE, NULL);

	CRUD_INITIALIZED = 0;

	free(table);

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... unmount complete.");
	return (0);
}

// Implementation
///////////////////////////////////////////////////////////////////////////////////
//                                                                          	 //
// Function     : crud_open                                                  	 //
// Description  : This function opens the file and returns a file handle     	 //
//                                                                             	 //
// Inputs       : path - the path "in the storage array"                      	 //
// Outputs      : file handle if successful, -1 if failure                    	 //
///////////////////////////////////////////////////////////////////////////////////
	
int16_t crud_open(char *path) 
{	
// Only interested in implementing for one file											1
// No file larger than CRUD MAX OBJECT SIZE											0
// Make an association between the name (sample) and the fle, and return a file descriptor					0
// file descriptor is a number used to represent the opening of a file								1
//	
// Have I initialized the CRUD interface?											1
// CRUD_CREATE: Recieves a block of data, a buffer,and a length, and return the new object ID in the response object structure	1
// Open, assign a file handler, make sure it hasn't been already called. 							1
// Think ahead of most error conditions												0
// File length is 0 when opening. 												1
	int iterator;
	int exist = 0;

	if(CRUD_INITIALIZED == 0) Construct_Crud_Response(CRUD_INIT, buffer);

	for(iterator=0; iterator < CRUD_MAX_TOTAL_FILES; iterator++)
	{
		if(strcmp(crud_file_table[iterator].filename, path)==0){
			fh = iterator;
			crud_file_table[fh].open = 1;
			exist = 1;
			fh = iterator;
			crud_file_table[fh].position = 0;
			return iterator;
		}
		
	}
	

	if(exist == 0){
		for(iterator=0; iterator < CRUD_MAX_TOTAL_FILES; iterator++){
			if(strcmp(crud_file_table[iterator].filename, "") == 0)
			{
				fh = iterator;
				strcpy(crud_file_table[fh].filename,path);
				crud_file_table[fh].open = 1;
				crud_file_table[fh].position = 0;
				fh = iterator;
				return iterator;
			}
		}
	}

	return -1;
}	
	
	
////////////////////////////////////////////////////////////////////////////////
//									      //
// Function     : crud_close						      //
// Description  : This function closes the file				      //
//									      //
// Inputs       : fd - the file handle of the object to close		      //
// Outputs      : 0 if successful, -1 if failure			      //
////////////////////////////////////////////////////////////////////////////////

int16_t crud_close(int16_t fh)
{
	// Throws away what we opened
	// Construct_Crud_Response(CRUD_DELETE, buffer);

	crud_file_table[fh].open = 0;

	return 0;
}

/////////////////////////////////////////////////////////////////////////////////
//									       //
// Function     : crud_read						       //
// Description  : Reads up to "count" bytes from the file handle "fh" into the //
//                buffer  "buf".					       //
//									       //
// Inputs       : fd - the file descriptor for the read			       //
//                buf - the buffer to place the bytes into		       //
//                count - the number of bytes to read			       //
// Outputs      : the number of bytes read or -1 if failures		       //	
/////////////////////////////////////////////////////////////////////////////////

int32_t crud_read(int16_t fd, void *buf, int32_t count) 
{
	// If file length is 0, position is also 0, return 0.				1
	// copy this into the buffer recieved						
	// object id assigned by object store, file descriptor assigned by me		2
	
	// Variable Declaration
	int32_t Bytesread;

	fh = fd;

	if(buf == NULL) return -1;

	// if the file length and the position are both 0 return 0 as the length
	if(count == 0 && crud_file_table[fd].position == 0) return 0;

	if(crud_file_table[fd].position == crud_file_table[fd].length) return 0;	
	
	// If the position was set to the end of the length, then return count;
	if(crud_file_table[fd].position == count)
	{
		return 0;	
	}
	
	else 
	{
		if(count > crud_file_table[fd].length - crud_file_table[fd].position) Bytesread = crud_file_table[fd].length - crud_file_table[fd].position;
		else Bytesread = count;
		 
		Construct_Crud_Response(CRUD_READ, buffer);
	
		memcpy(buf, &buffer[crud_file_table[fd].position], Bytesread);

		// Another check just in case the above one is faulty
		if(check_crud_response <<63 >> 63 != 0)
			return -1;
	
		crud_file_table[fd].position = crud_file_table[fd].position + Bytesread;
	
		return Bytesread;
	}
	
}

//////////////////////////////////////////////////////////////////////////////////////////
//											//
// Function     : crud_write								//
// Description  : Writes "count" bytes to the file handle "fh" from the			//
//                buffer  "buf"								//
//											//
// Inputs       : fd - the file descriptor for the file to write to			//
//                buf - the buffer to write						//
//                count - the number of bytes to write					//
// Outputs      : the number of bytes written or -1 if failure				//
//////////////////////////////////////////////////////////////////////////////////////////

int32_t crud_write(int16_t fd, void *buf, int32_t count)
{
	// The buffer is given to me. We assume it has at least as many bytes as count
	// At the current file position read (count number) of bytes. If there are less than count number of files
	// return the number of bytes read, 0 being the end of the file. Error value is -1
	// Middle of the file: overwrite																	0
	// At the end of file: Append to the end of the file															0
	// Begginning of the file: append																	0
	// CREATE.OBJECT and is sent to CRUD.INIT																?
	// CRUD.INIT returns 																			?
	// position equal to the last position in the array that was written													0
	// When returning from one of the functions, cannot withhold other information other than the file descriptor, position and object ID.					0
	// CRUD.delete the old object and creates a new object (cannot change size, appending copies the object info to a new object and deletes the previous one)		0
	// If file id is non valid or not in range, return -1 (error)														0
	
	// Temporary length placeholder
	int32_t length;

	buf = (char *)buf;
	
	fh = fd;

	if(buf == NULL) return -1;

	if(CRUD_INITIALIZED == 0) Construct_Crud_Response(CRUD_INIT, buf);
	if(fh == -1) return -1;
	if(crud_file_table[fd].length == 0 && count ==0) return 0;

	// If the object is not initialized (NULL object ID)
	if(crud_file_table[fd].object_id == CRUD_NO_OBJECT)
	{
		crud_file_table[fd].length = count;

		Construct_Crud_Response(CRUD_CREATE, buf);


	}

	// If the object ID does exist, the file has been initialized
	if(crud_file_table[fd].object_id != CRUD_NO_OBJECT)
	{
		if(crud_file_table[fd].position < crud_file_table[fd].length)
		{

			// If object size is less than the new input size
			if(crud_file_table[fd].length < count + crud_file_table[fd].position)
			{
				char *buff = malloc(count + crud_file_table[fd].position);

				length = crud_file_table[fd].position + count;

				Construct_Crud_Response(CRUD_READ, buff);

				memcpy(buffer, buff, length);

				Construct_Crud_Response(CRUD_DELETE, NULL);

				crud_file_table[fd].length = length;

				Construct_Crud_Response(CRUD_CREATE, buffer);

				memcpy(buffer, buff, crud_file_table[fd].position);

				memcpy(&buffer[crud_file_table[fd].position], buf, count);

				Construct_Crud_Response(CRUD_UPDATE, buffer);

				crud_file_table[fd].position = crud_file_table[fd].length;

				free(buff);

				return count;
			}

			// If object length is greater than new input
			else if(crud_file_table[fd].length > count + crud_file_table[fd].position)
			{
				Construct_Crud_Response(CRUD_READ, buffer);

				memcpy(&buffer[crud_file_table[fd].position], buf, count); 

				Construct_Crud_Response(CRUD_UPDATE, buffer);

				crud_file_table[fd].position = crud_file_table[fd].position + count;

				return count;
			}

			//if size doesn't change
			else if(crud_file_table[fd].length == count + crud_file_table[fd].position)
			{
				char *buff = malloc(count + crud_file_table[fd].position);

				//Read bytes
				Construct_Crud_Response(CRUD_READ, buff);

				length = crud_file_table[fd].position + count;

				memcpy(buffer, buff, length);

				Construct_Crud_Response(CRUD_DELETE, NULL);

				crud_file_table[fd].length = length;

				Construct_Crud_Response(CRUD_CREATE, buffer);

				memcpy(buffer, buff, crud_file_table[fd].position);
				memcpy(&buffer[crud_file_table[fd].position], buf, count);

				// Update the object
				Construct_Crud_Response(CRUD_UPDATE, buffer);

				crud_file_table[fd].position = crud_file_table[fd].length;

				free(buff);

				return count;
			}

		}

		// If bytes are being written at the end of file
		if(crud_file_table[fd].position == crud_file_table[fd].length && crud_file_table[fd].position != 0)
		{
			char *buff = malloc(count + crud_file_table[fd].position );

			Construct_Crud_Response(CRUD_READ, buff);

			memcpy(buffer, buff, crud_file_table[fd].position);

			length = count + crud_file_table[fd].position;

			Construct_Crud_Response(CRUD_DELETE, NULL);

			// Restore object length
			crud_file_table[fd].length = length;

			Construct_Crud_Response(CRUD_CREATE, buffer);

			memcpy(buffer, buff, crud_file_table[fd].position);

			memcpy(&buffer[crud_file_table[fd].position], buf, count);

			Construct_Crud_Response(CRUD_UPDATE, buffer);

			crud_file_table[fd].position = crud_file_table[fd].length;

			free(buff);

			return count;


		}

	}
	return count;
}

//////////////////////////////////////////////////////////////////////////////////
//									      	//
// Function     : crud_seek						       	//
// Description  : Seek to specific point in the file			     	//
//										//
// Inputs       : fd - the file descriptor for the file to seek			//
//                loc - offset from beginning of file to seek to		//
// Outputs      : 0 if successful or -1 if failure				//
//////////////////////////////////////////////////////////////////////////////////

int32_t crud_seek(int16_t fd, uint32_t loc)
{
	// Go to a specific point in the bounds
	// Error: Seeking out of bounds
	// Changes file position
	// Cannot be less than 0 or greater than the length of the file

	// If the position is out of range, return -1
	if(loc < 0 || loc > crud_file_table[fd].length)
		return -1;

	else
		crud_file_table[fd].position = loc;

	// Return the new position
	return 0;
}

//////////////////////////////////////////////////////////////////////////////////
//										//
// Function     : crudIOUnitTest						//
// Description  : Perform a test of the CRUD IO implementation			//
//										//
// Inputs       : None								//
// Outputs      : 0 if successful or -1 if failure				//
//////////////////////////////////////////////////////////////////////////////////

int crudIOUnitTest(void) {

	// Local variables
	uint8_t ch;
	int16_t fh, i;
	int32_t cio_utest_length, cio_utest_position, count, bytes, expected;
	char *cio_utest_buffer, *tbuf;
	CRUD_UNIT_TEST_TYPE cmd;
	char lstr[1024];

	// Setup some operating buffers, zero out the mirrored file contents
	cio_utest_buffer = malloc(CRUD_MAX_OBJECT_SIZE);
	tbuf = malloc(CRUD_MAX_OBJECT_SIZE);
	memset(cio_utest_buffer, 0x0, CRUD_MAX_OBJECT_SIZE);
	cio_utest_length = 0;
	cio_utest_position = 0;

	// Format and mount the file system
	if (crud_format() || crud_mount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on format or mount operation.");
		return(-1);
	}

	// Start by opening a file
	fh = crud_open("temp_file.txt");
	if (fh == -1) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure open operation.");
		return(-1);
	}

	// Now do a bunch of operations
	for (i=0; i<CRUD_IO_UNIT_TEST_ITERATIONS; i++) {

		// Pick a random command
		if (cio_utest_length == 0) {
			cmd = CIO_UNIT_TEST_WRITE;
		} else {
			cmd = getRandomValue(CIO_UNIT_TEST_READ, CIO_UNIT_TEST_SEEK);
		}

		// Execute the command
		switch (cmd) {

		case CIO_UNIT_TEST_READ: // read a random set of data
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d at position %d", bytes, cio_utest_position);
			bytes = crud_read(fh, tbuf, count);
			if (bytes == -1) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Read failure.");
				return(-1);
			}

			// Compare to what we expected
			if (cio_utest_position+count > cio_utest_length) {
				expected = cio_utest_length-cio_utest_position;
			} else {
				expected = count;
			}
			if (bytes != expected) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : short/long read of [%d!=%d]", bytes, expected);
				return(-1);
			}
			if ( (bytes > 0) && (memcmp(&cio_utest_buffer[cio_utest_position], tbuf, bytes)) ) {

				bufToString((unsigned char *)tbuf, bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST R: %s", lstr);
				bufToString((unsigned char *)&cio_utest_buffer[cio_utest_position], bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST U: %s", lstr);

				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : read data mismatch (%d)", bytes);
				return(-1);
			}
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d match", bytes);


			// update the position pointer
			cio_utest_position += bytes;
			break;

		case CIO_UNIT_TEST_APPEND: // Append data onto the end of the file
			// Create random block, check to make sure that the write is not too large
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			if (cio_utest_length+count >= CRUD_MAX_OBJECT_SIZE) {

				// Log, seek to end of file, create random value
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : append of %d bytes [%x]", count, ch);
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", cio_utest_length);
				if (crud_seek(fh, cio_utest_length)) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", cio_utest_length);
					return(-1);
				}
				cio_utest_position = cio_utest_length;
				memset(&cio_utest_buffer[cio_utest_position], ch, count);

				// Now write
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes != count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : append failed [%d].", count);
					return(-1);
				}
				cio_utest_length = cio_utest_position += bytes;
			}
			break;

		case CIO_UNIT_TEST_WRITE: // Write random block to the file
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			// Check to make sure that the write is not too large
			if (cio_utest_length+count < CRUD_MAX_OBJECT_SIZE) {
				// Log the write, perform it
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : write of %d bytes [%x]", count, ch);
				memset(&cio_utest_buffer[cio_utest_position], ch, count);
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes!=count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : write failed [%d].", count);
					return(-1);
				}
				cio_utest_position += bytes;
				if (cio_utest_position > cio_utest_length) {
					cio_utest_length = cio_utest_position;
				}
			}
			break;

		case CIO_UNIT_TEST_SEEK:
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", count);
			if (crud_seek(fh, count)) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", count);
				return(-1);
			}
			cio_utest_position = count;
			break;

		default: // This should never happen
			CMPSC_ASSERT0(0, "CRUD_IO_UNIT_TEST : illegal test command.");
			break;

		}

#if DEEP_DEBUG
		// VALIDATION STEP: ENSURE OUR LOCAL IS LIKE OBJECT STORE
		CrudRequest request;
		CrudResponse response;
		CrudOID oid;
		CRUD_REQUEST_TYPES req;
		uint32_t length;
		uint8_t res, flags;

		// Make a fake request to get file handle, then check it
		request = construct_crud_request(crud_file_table[0].object_id, CRUD_READ, CRUD_MAX_OBJECT_SIZE, CRUD_NULL_FLAG, 0);
		response = crud_client_operation(request, tbuf);
		if ((deconstruct_crud_request(response, &oid, &req, &length, &flags, &res) != 0) || (res != 0))  {
			logMessage(LOG_ERROR_LEVEL, "Read failure, bad CRUD response [%x]", response);
			return(-1);
		}
		if ( (cio_utest_length != length) || (memcmp(cio_utest_buffer, tbuf, length)) ) {
			logMessage(LOG_ERROR_LEVEL, "Buffer/Object cross validation failed [%x]", response);
			bufToString((unsigned char *)tbuf, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VR: %s", lstr);
			bufToString((unsigned char *)cio_utest_buffer, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VU: %s", lstr);
			return(-1);
		}

		// Print out the buffer
		bufToString((unsigned char *)cio_utest_buffer, cio_utest_length, (unsigned char *)lstr, 1024 );
		logMessage(LOG_INFO_LEVEL, "CIO_UTEST: %s", lstr);
#endif

	}

	// Close the files and cleanup buffers, assert on failure
	if (crud_close(fh)) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure read comparison block.", fh);
		return(-1);
	}
	free(cio_utest_buffer);
	free(tbuf);

	// Format and mount the file system
	if (crud_unmount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on unmount operation.");
		return(-1);
	}

	// Return successfully
	return(0);
}

































