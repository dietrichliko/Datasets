# Datasets from StopsCompressed Analysis

* AOD
* MiniAOD
* NanoAD
* ReNanoAOD: 
    * Enriched NanoAOD 
    * Datasets in Vienna on Priyas EOS area
* Ntuples for StopsCompressed:
    * Postprocessed NanoAOD. Contains subset of the NanoAOD (and other variables)  
    * Dataset in Vienna on my EOS area

## Run II UL datasets

[Google Sheet](https://docs.google.com/spreadsheets/d/1lypCxC2wT9T0OFC_B3xvfOmVez9vXrI8viCT7UhxLYM ) from Priya, which contains all 
information required for processing.

[UL datasets for the StopsCompressed Analysis](https://github.com/HephyAnalysisSW/Samples/tree/StopsCompressedUL)
* [Data 2016preVFP](https://github.com/HephyAnalysisSW/Samples/blob/StopsCompressedUL/nanoAOD/python/Run2016APV_private_ULnanoAODv9.py)
* [Data 2016postVFP](https://github.com/HephyAnalysisSW/Samples/blob/StopsCompressedUL/nanoAOD/python/Run2016_private_ULnanoAODv9.py)
* [Data 2017](https://github.com/HephyAnalysisSW/Samples/blob/StopsCompressedUL/nanoAOD/python/Run2017_private_ULnanoAODv9.py)
* [Data 2018](https://github.com/HephyAnalysisSW/Samples/blob/StopsCompressedUL/nanoAOD/python/Run2018_private_ULnanoAODv9.py)
* [MC 2016preVFP](https://github.com/HephyAnalysisSW/Samples/blob/StopsCompressedUL/nanoAOD/python/UL16APVv9_private.py)
* [MC 2016postVFP](https://github.com/HephyAnalysisSW/Samples/blob/StopsCompressedUL/nanoAOD/python/UL16v9_private.py)
* [MC 2017](https://github.com/HephyAnalysisSW/Samples/blob/StopsCompressedUL/nanoAOD/python/UL17v9_private.py)
* [MC 2018](https://github.com/HephyAnalysisSW/Samples/blob/StopsCompressedUL/nanoAOD/python/UL18v9_private.py)

[My Google Doc](https://docs.google.com/spreadsheets/d/1ddNADBoH1f-bL9faXes15c4hPGvB9ugLOv_7VvinKL8): Still incomplete summary of from the above sources

## Tools

Simple tool to list and stage datasets. It uses the information from my google doc to list and stage files. 
You can find the "nice" name in the spreadsheet in the column Datasets. At this point only some datasets of the
period "Run2016preVFP" have been entered. The others will follow.

List the content of the dataset MET_Run2016C_HIPM_UL in the Ntuple tier
   
    $ ./datasets.py list MET_Run2016C_HIPM_UL --tier=ntuple
    root://eos.grid.vbc.ac.at//store/user/liko/StopsCompressed/nanoTuples/compstops_UL16APVv9_nano_v10/Met/MET_Run2016C_HIPM_UL/MET_Run2016C_HIPM_UL_0.root
    root://eos.grid.vbc.ac.at//store/user/liko/StopsCompressed/nanoTuples/compstops_UL16APVv9_nano_v10/Met/MET_Run2016C_HIPM_UL/MET_Run2016C_HIPM_UL_1.root
    .....

Stage the first file of the dataset DYJetsToLL_M50_HT70to100 from the MiniAOD tier on the scratch area.

    $ ./datasets.py stage DYJetsToLL_M50_HT70to100 --tier=MiniAOD --max-files=1
    /scratch-cbe/users/dietrich.liko/cache/mc/RunIISummer20UL16MiniAODAPVv2/DYJetsToLL_M-50_HT-70to100_TuneCP5_PSweights_13TeV-madgraphMLM-pythia8/MINIAODSIM/106X_mcRun2_asymptotic_preVFP_v11-v2/2430000/B685E9BC-7B62-BB4A-8A53-D51CB6DB8133.root