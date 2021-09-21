import sys
from os import listdir


def format():
    dir = "/Users/sakthimurugan/downloads/d018-2017/"
    formatted_dir = "/Users/sakthimurugan/downloads/formatted_d018-2017/"
    files = listdir(dir)

    for file in files:
        filename = dir + file
        f_filename = formatted_dir + file
        with open(f_filename, "w+") as ff:
            for line in open(filename):
                line1 = line.replace("\n", "|\n")
                ff.writelines(line1)


if __name__ == "__main__":
    format()
