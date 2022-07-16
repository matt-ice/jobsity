# Challenge

Solution was created using FastAPI with a local SQLite database as storage while having some code 90% complete for potential AWS implementation

## How to run
The requirements.txt file is included for all the dependancies and the easiest way to test it is to install uvicorn by running pip install uvicorn from the terminal.
Run it from terminal by entering uvicorn --host 0.0.0.0 and adding --reload if you plan to make changes to the code during testing

## Next steps for improvement
Dockerize the solution
Test and optimize chunk loading performance into various databases

## Cloud proposals on AWS
1. The API would be deployed to an EC2 instance and run from there
2. The API would be deployed to AWS by Elastic Beanstalk for a fully managed environment
3. Transformation layer could live in AWS Lambda and would be triggered by an event notification from S3 and the response layer would live in EC2

