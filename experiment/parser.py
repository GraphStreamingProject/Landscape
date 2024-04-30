import sys

MiB = 1024 * 1024
GiB = MiB * 1024

if __name__ == "__main__":
  for file_name in sys.argv[1:]:
    print("File:", file_name)
    with open(file_name) as input_file:
      lines = input_file.readlines()

      eth0_1 = lines[3].rstrip().split()
      beg_recv = int(eth0_1[1])
      beg_sent = int(eth0_1[9])

      eth0_2 = lines[len(lines) - 1].rstrip().split()
      end_recv = int(eth0_2[1])
      end_sent = int(eth0_2[9])

      bytes_recv = end_recv - beg_recv
      bytes_sent = end_sent - beg_sent

      print("Total bytes recieved(GiB): ", round(bytes_recv / GiB, 3))
      print("Total bytes sent(GiB):     ", round(bytes_sent / GiB, 3))
      print("Total overall(GiB):        ", round((bytes_recv + bytes_sent) / GiB, 3))
      for line in lines[4:len(lines) - 8 + 4]:
        print(line.rstrip())
      print()
