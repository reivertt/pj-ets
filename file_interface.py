import os
import json
import base64
from glob import glob


class FileInterface:
    def __init__(self):
        os.chdir('files/')

    def list(self,params=[]):
        try:
            filelist = glob('*.*')
            return dict(status='OK',data=filelist)
        except Exception as e:
            return dict(status='ERROR',data=str(e))

    def get(self,params=[]):
        try:
            filename = params[0]
            if (filename == ''):
                return None
            fp = open(f"{filename}",'rb')
            isifile = base64.b64encode(fp.read()).decode()
            return dict(status='OK',data_namafile=filename,data_file=isifile)
        except Exception as e:
            return dict(status='ERROR',data=str(e))
        
    def post(self, params=[]):
        try:
            filename = params[0]
            filedata = params[1]
            file_content = base64.b64decode(filedata)
            fp = open(filename, 'wb+')
            fp.write(file_content)
            fp.close()
            return dict(status='OK', data=f'File {filename} successfully uploaded')
        except Exception as e:
            return dict(status='ERROR', data=str(e))
    
    def delete(self, params=[]):
        try:
            filename = params[0]
            if os.path.exists(filename):
                os.remove(filename)
                return dict(status='OK', data=f'File {filename} successfully deleted')
            else:
                return dict(status='ERROR', data=f'File {filename} does not exist')
        except Exception as e:
            return dict(status='ERROR', data=str(e))



if __name__=='__main__':
    f = FileInterface()
    print(f.list())
    print(f.get(['pokijan.jpg']))
