from decimal import Decimal


class KalmanFilter:
    def __init__(self):
        self.last_p = Decimal(0.02)
        self.current_p = Decimal(0)
        self.out = Decimal(0)
        self.k_gain = Decimal(0)
        self.q = Decimal(0.075)
        self.r = Decimal(0.6)

    def __kalman_filter(self, data_point: Decimal):
        self.current_p = Decimal(self.last_p) + Decimal(self.q)
        self.k_gain = self.current_p / (self.current_p + self.r)
        self.out = self.out + self.k_gain * (data_point - self.out)
        self.last_p = (1 - self.k_gain) * self.current_p
        return self.out

    def flush_data(self, data, col):
        for data_point in data:
            self.__kalman_filter(data_point[col])

    def calc_new_data(self, data):
        return self.__kalman_filter(data)

    def set_q(self, value: Decimal):
        self.q = value

    def set_default_q(self):
        self.q = Decimal(0.075)
