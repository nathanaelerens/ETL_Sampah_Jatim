import pandas as pd 
import os 


Timbulan_sampah = pd.read_excel('Data_Timbulan_Sampah_SIPSN_KLHK_Jawa Timur_2024.xlsx', skiprows=1) 


komposisi_sampah = pd.read_excel('Data_Komposisi_Jenis_Sampah_SIPSN_KLHK_Jawa Timur_2024.xlsx', skiprows=1)

if os.path.exists('timbulan_sampah_jatim.csv') or os.path.exists('komposisi_sampah_jatim.csv'):
    os.remove('timbulan_sampah_jatim.csv')
    os.remove('komposisi_sampah_jatim.csv')

    Timbulan_sampah.to_csv('timbulan_sampah_jatim.csv', index=False)
    komposisi_sampah.to_csv('komposisi_sampah_jatim.csv', index=False)
else:
    Timbulan_sampah.to_csv('timbulan_sampah_jatim.csv', index=False)
    komposisi_sampah.to_csv('komposisi_sampah_jatim.csv', index=False)