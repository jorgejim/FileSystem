//////////////////////////////////////////////////////////////////////////////////
//										//
//  File          : crud_client.c						//
//  Description   : This is the client side of the CRUD communication protocol.	//
//										//
//   Author       : Jorge Jimenez						//
//  Last Modified : Mon Dec 1st  06:59:59 EDT 2014				//
//////////////////////////////////////////////////////////////////////////////////

// Include Files
#include <stdint.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

// Project Include Files
#include <crud_network.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>

// Global variables
int crud_network_shutdown = 0; // Flag indicating shutdown
unsigned char *crud_network_address = NULL; // Address of CRUD server 
unsigned short crud_network_port = 0; // Port of CRUD server
int isConnected = 0;
int socketFD;

//
// Functions

int Connect()
{

	// Getting an Address

	// Variables
	//int /*result, result2,*/ socketFD;
	struct sockaddr_in caddr;

	// Create a socket
	
	socketFD = socket(AF_INET, SOCK_STREAM, 0);
	if(socketFD == -1)
	{
		printf("Error on socket creation \n");
		return -1;
	}

	// Connect to server
	
	//Setup the address information
	caddr.sin_family = AF_INET;
	caddr.sin_port = htons(CRUD_DEFAULT_PORT);
	
	if(inet_aton(CRUD_DEFAULT_IP, &caddr.sin_addr) == 0)
		return -1;
	
	if( connect(socketFD, (const struct sockaddr *) &caddr, sizeof(struct sockaddr)) == -1)
		return -1;

	isConnected = 1;

	return 0;
}

int Send(uint64_t request, char *buffer)
{
	// Variables
	int result, position = 0;
	uint64_t requestval;
	uint32_t count, amWritten, bytesTotal;

	bytesTotal = sizeof(uint64_t);
	
	// If no connection has been made, connect to server
	if(isConnected == 0)
		result = Connect();

	if( result == -1) 
		printf("Could not Connect to Server\n");

	requestval = request << 32 >> 60;
	count = request << 36 >> 40;
	request = htonll64(request);

	while(amWritten != bytesTotal)
	{
		// Loop to make sure the whole 64bit request is passed in (should pass in the first iteration)
		amWritten = write(socketFD, &request + position, sizeof(request) - position);// count or number of bytes in command (4)
		bytesTotal = bytesTotal - amWritten;
		position = position + amWritten;
		amWritten = 0;
	}

	// Reset bytesTotal
	bytesTotal = count;

	// Reset position
	position = 0;

	// If the command sent in is either CREATE or UPDATE then send in the buffer too
	if(requestval == CRUD_CREATE || requestval == CRUD_UPDATE)
	{
		// Loop to make sure all bytes are being written
		while(amWritten != bytesTotal)
		{
			amWritten = write(socketFD, &buffer[position], count);
			if(amWritten == -1)
				result = -1;
			bytesTotal = bytesTotal - amWritten;
			position = position + amWritten;
			count = count - position;
			amWritten = 0;
		}
	}
	return 0;
}

uint64_t Recieve(char *buffer)
{
	// Variables
	uint32_t length, bytesTotal;
	int requestcmd, position = 0;// result;
	uint32_t amRead;
	uint64_t readinto;

	if (isConnected == 0)
		printf("Error, not connected to server");

	// If client is connected to the server
	else{
		//recieve response value
	
		bytesTotal = sizeof(uint64_t);

		// Loop to make sure all bytes are read
		while(amRead != bytesTotal)
		{	
			amRead = read(socketFD, &readinto + position, bytesTotal);
			bytesTotal = bytesTotal - amRead;
			position = position + amRead;
			amRead = 0;
		}
		
		// Turn 64bit value recieved into host order
		readinto = ntohll64(readinto);
		// Extract the length from the 64bit value
		length = readinto << 36 >> 40;
		// Request the command from the 64bit value
		requestcmd = readinto << 32 >> 60;
		
		// Reset variables
		position = 0;
		bytesTotal = length;
		
		if(requestcmd == CRUD_READ)
		{
			// Loop to make sure the whole object is read
			while(amRead != bytesTotal)
			{
				// Assigning amount read to amRead 
				amRead = read(socketFD, &buffer[position], length);
				bytesTotal = bytesTotal - amRead;
				position = position + amRead;
				length = length - position;
				amRead = 0;
			}
		}
	}

	return readinto;
}

//////////////////////////////////////////////////////////////////////////////////
//										//
// Function     : crud_client_operation						//
// Description  : This the client operation that sends a request to the CRUD	//
//                server.   It will:						//
//										//
//                1) if INIT make a connection to the server			//
//                2) send any request to the server, returning results		//
//                3) if CLOSE, will close the connection			//
//										//
// Inputs       : op - the request opcode for the command			//
//                buf - the block to be read/written from (READ/WRITE)		//
// Outputs      : the response structure encoded as needed			//
//////////////////////////////////////////////////////////////////////////////////


CrudResponse crud_client_operation(CrudRequest op, void *buf)
{
	// Variables
	int result;
	uint64_t response;
	buf = (char *)buf;

	// If the comman sent in is CRUD_INIT start the connection to the server
	if(op <<32 >> 60 == CRUD_INIT && isConnected == 0)
		result = Connect();

	// Error checking
	if(result == -1) 
		printf("Error Connecting");

	// Assign return value of sent bytes to result to error check
	result = Send(op, buf);
	
	if(op << 32 >> 60 == CRUD_CLOSE)
	{
		close(socketFD);
		socketFD = -1;
		isConnected = 0;
	}

	// Error checking
	if(result == -1) 
		printf("Error sending\n");

	// Store in temporary 64bit value to return the converted resonse
	response = Recieve(buf);

	return response;
}

