# main.py

from custom_vpc import create_vpc_for_databricks
from s3_role import main
if __name__ == "__main__":
    create_vpc_for_databricks()
    main()

