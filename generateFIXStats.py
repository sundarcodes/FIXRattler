import fileinput;

for file in fileinput.input():
  print(file)
  mbrId=file[4:3];
  #print(mbrId);
