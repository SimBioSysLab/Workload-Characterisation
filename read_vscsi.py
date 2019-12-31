from PyMimircache import Cachecow

READ_OP_CODES = [40, 8, 168, 136, 9]
WRITE_OP_CODES = [42, 10, 170, 138, 11]


def read_file(file_name):

    if "vscsi1" in file_name:
        trace_type = 1
    else:
        trace_type = 2

    c = Cachecow()
    reader = c.vscsi(file_path=file_name, vscsi_type=trace_type)

    data = reader.read_complete_req()
    while data:
        print(data)
        data = reader.read_complete_req()


if __name__ == '__main__':
    file_name = "dataset/cloudphysics/w17_vscsi1.vscsitrace"
    read_file(file_name)
