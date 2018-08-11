import numpy as np


def change_freq(data, new_freq):
    if np.ndim(data) == 3:
        if data.shape[1] % new_freq != 0:
            raise ValueError("new_freq %d Cannot be divisible by %d." % (new_freq, data.shape[1]))

        temp_data = data.reshape((-1, 5))

        open_index = np.arange(0, int(temp_data.shape[0]), new_freq)
        close_index = open_index+new_freq-1

        open_price = temp_data[open_index, 0]
        high_price = np.max(temp_data[:, 1].reshape((-1, new_freq)), axis=1)
        low_price = np.min(temp_data[:, 2].reshape((-1, new_freq)), axis=1)
        close_price = temp_data[close_index, 3]
        volume = np.sum(temp_data[:, 4].reshape((-1, new_freq)), axis=1)

        new_data = np.array([open_price, high_price, low_price, close_price, volume])

        return new_data.T.reshape((-1, int(data.shape[1]/new_freq), 5))

    if np.ndim(data) == 2:
        if data.shape[0] % new_freq != 0:
            raise ValueError("new_freq %d Cannot be divisible by %d." % (new_freq, data.shape[0]))

        open_index = np.arange(0, int(data.shape[0]), new_freq)
        close_index = open_index+new_freq-1

        open_price = data[open_index, 0]
        high_price = np.max(data[:, 1].reshape((-1, new_freq)), axis=1)
        low_price = np.min(data[:, 2].reshape((-1, new_freq)), axis=1)
        close_price = data[close_index, 3]
        volume = np.sum(data[:, 4].reshape((-1, new_freq)), axis=1)

        new_data = np.array([open_price, high_price, low_price, close_price, volume])

        return new_data.T


if __name__ == "__main__":
    path = "D:/hjq/Python/stock_min_xy/1-x_data.npy"
    new_freq = 15

    data = np.load(path)
    temp_data = data.reshape((-1, 5))
    print(data.shape)
    print(temp_data.shape)

    a = change_freq(data=data, new_freq=new_freq)
    print(a.shape)

    b = change_freq(data=temp_data, new_freq=new_freq)
    print(b.shape)










