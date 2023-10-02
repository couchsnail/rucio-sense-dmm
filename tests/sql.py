import sys
sys.path.append("..")

import unittest
import random
from dmm.db.session import get_engine, get_maker, get_session, databased
from dmm.db.models import Request, FTSTransfer
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine import Engine as engine

# Import the functions and decorator from your session.py module

class TestSession(unittest.TestCase):
    def test_databased_decorator(self):
        # Define a sample function to be used with the decorator
        @databased
        def sample_function(session):
            # Your logic using the session goes here
            req = Request(request_id=f"Test_{random.random()}")
            
            transfer1 = FTSTransfer()
            transfer1.value = "testid_1"
            
            transfer2 = FTSTransfer()
            transfer2.value = "testid_2"
            
            req.transfer_ids = [transfer1, transfer2]
            req.save(session)

            new_attrs = {
                "src_site": "A",
                "dst_site": "B"
            }

            req.update(new_attrs)
            print(session.query(Request).first().__dict__)
        # Call the sample function with the decorator
        sample_function()


if __name__ == "__main__":
    unittest.main()
