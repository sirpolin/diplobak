from sqlalchemy import Column, TIMESTAMP, NVARCHAR, Text, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Response(Base):
    __tablename__ = 'response'
    response_id = Column(Integer, primary_key=True)
    last_update = Column(TIMESTAMP)
    bank_name = Column(NVARCHAR)
    city = Column(NVARCHAR)
    title = Column(Text)
    fulltext = Column(Text)
    datetime = Column(TIMESTAMP)
    mark = Column(Integer)
    num_views = Column(Integer)
    num_comments = Column(Integer)
    status = Column(NVARCHAR)
    debitcards = Column(Boolean)
    creditcards = Column(Boolean)
    hypothec = Column(Boolean)
    autocredits = Column(Boolean)
    credits = Column(Boolean)
    restructing = Column(Boolean)
    deposits = Column(Boolean)
    investments = Column(Boolean)
    transfers = Column(Boolean)
    remote = Column(Boolean)
    corporate = Column(Boolean)
    rko = Column(Boolean)
    acquiring = Column(Boolean)
    salary_project = Column(Boolean)
    businessdeposits = Column(Boolean)
    businesscredits = Column(Boolean)
    bank_guarantee = Column(Boolean)
    leasing = Column(Boolean)
    business_other = Column(Boolean)
    business_remote = Column(Boolean)

    def __init__(self, response_id, product_name):
        self.response_id = response_id
        setattr(self, product_name, True)