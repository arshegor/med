import glob
import os
import pydicom
import shutil

def load_dicom(path):
    return pydicom.dcmread(path)


def create_dir(meta):
    name_for_pdf = (meta['InstitutionAddress'] + " "
    + meta['StudyDate'] + " "
    + meta['PatientsName']
    + meta['PatientsBirthDate']
    )
    
    return name_for_pdf.replace(" ", "_").replace(".", "_").replace(",", "")


def put_file_to_directory(file, path):
    try:
        os.mkdir(path)
    except:
        pass

    shutil.copy(file, path)

    
def get_dicom_meta(file):
    meta = dict()

    meta.update({'InstitutionAddress' : file.InstitutionAddress})
    meta.update({'PatientsName' : str(file.PatientName)})

    meta.update({'PatientsBirthDate':file.PatientBirthDate})
    meta.update({'StudyDate' : file.StudyDate})
    
    return meta


def main():
    metaList = list()
    os.chdir("/mnt/data/dicoogle")

    
    for file in glob.glob("*/*/*/*/*/*/*.dcm"):
        path = "/home/mkiit/patients/"

        dcm = load_dicom(file)
        meta = get_dicom_meta(dcm)
        dirName = create_dir(meta)
        put_file_to_directory(file, path+dirName)
        metaList.append(meta)
        
    # print(metaList)



main()