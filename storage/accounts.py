from sqlalchemy import Column, Integer, String, DateTime
from base import Base
from datetime import datetime


class Account(Base):
    """ Account """
    __tablename__ = "account"
    __table_args__ = {'mysql_engine':'InnoDB'}

    accountID = Column(Integer, primary_key=True)
    holding = Column(String(250)) #?????? how to handle object?
    cash = Column(Integer)
    value = Column(Integer)
    accountType = Column(String(250))
    currencyID = Column(Integer)
    createdAt = Column(DateTime)
    traceID = Column(String(250))

    def __init__(self, accountID, holding, cash, value, accountType, currencyID, createdAt, traceID):
        """ Initializes an account """
        self.accountID = accountID
        self.holding = holding
        self.cash = cash
        self.value = value
        self.accountType = accountType
        self.currencyID = currencyID
        self.createdAt = createdAt
        self.traceID = traceID

    def to_dict(self):
        """ Dictionary Representation of an account """
        dict = {}
        dict['accountID'] = self.accountID
        dict['holding'] = self.holding
        dict['cash'] = self.cash
        dict['value'] = self.value
        dict['accountType'] = self.accountType
        dict['currencyID'] = self.currencyID
        dict['createdAt'] = self.createdAt
        dict['traceID'] = self.traceID

        return dict
