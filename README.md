## Getting Started

To start using the program, ensure that Databricks and AWS are connected and that all ECS dependencies are created with the appropriate IAM rules. After verifying this setup, you can edit your `.env` files to include all the required information necessary for using the program.

RUN

pip install -r requirements.txt

### IAM Policy for Databricks EC2

Include the following IAM policy in your main Databricks EC2 IAM role:

```json
{
  "Effect": "Allow",
  "Action": [
    "ec2:DescribeNetworkAcls",
    "ec2:DescribeVpcAttribute"
  ],
  "Resource": "*"
}
