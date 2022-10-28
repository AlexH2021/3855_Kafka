from sqlalchemy import Column, Float, Integer, String
from base import Base


class Account_Stats(Base):
    """ Account """
    __tablename__ = "acc_stats"

    id = Column(Integer, primary_key=True)
    total_acc_num = Column(Integer)
    total_cash = Column(Float)
    total_value = Column(Float)
    traceID = Column(String(250), nullable=True)

    def __init__(self, total_acc_num, cash, value, traceID=0):
        """ Initializes an account """
        self.total_acc_num = total_acc_num
        self.total_cash = cash
        self.total_value = value
        self.traceID = traceID