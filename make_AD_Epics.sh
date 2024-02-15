#!/bin/bash


mkdir $HOME/epics
cd $HOME/epics

git clone https://github.com/epics-base/epics-base.git
cd epics-base
echo -e "\e[32mBuilding EPICS\e[0m"
make -sj

echo -e "\e[32mEPICS BUILD\e[0m"
sleep 2

export EPICS_BASE=$HOME/epics/epics-base

cd $HOME/epics
pwd


sleep 2
wget https://github.com/mittoalb/Scripts_epics/blob/main/assemble_synApps.sh

echo -e "\e[32mBuilding synApps\e[0m"

chmod +x assemble_synApps.sh
./assemble_synApps.sh



file="$HOME/epics/synApps/support/Makefile"
missing_module=false

######################################CHECK MODULES################################################################
# List of modules to check for
modules=("ALLEN_BRADLEY" "ALIVE" "AREA_DETECTOR" "ASYN" "AUTOSAVE" "BUSY" "CALC" "CAMAC" "CAPUTRECORDER" "DAC128V"
         "DELAYGEN" "DEVIOCSTATS" "DXP" "DXPSITORO" "ETHERIP" "IPAC" "IP" "IP330" "IPUNIDIG" "LOVE" "LUA" "MCA"
         "MEASCOMP" "MODBUS" "MOTOR" "OPTICS" "QUADEM" "SNCSEQ" "SOFTGLUE" "SOFTGLUEZYNQ" "SSCAN" "STD" "STREAM"
         "VAC" "VME" "XXX" "YOKOGAWA_DAS" "IOCSTATS")

# Iterate over each module and check its presence in the file
for module in "${modules[@]}"; do
    if grep -q "$module" "$file"; then
        echo "Module $module found in $file"
    else
        echo "Module $module not found in $file"
        missing_module=true
    fi
done

# Exit the script if any module is missing
if [ "$missing_module" = true ]; then
    echo "Exiting script due to missing module(s)"
    exit 1
fi

######################################CHECK MODULES################################################################

file="$HOME/epics/synApps/support/configure/RELEASE"
missing_module=false

# List of modules to check for
modules=("ASYN=\$(SUPPORT)/asyn-R4-42" "AUTOSAVE=\$(SUPPORT)/autosave-R5-10-2" "BUSY=\$(SUPPORT)/busy-R1-7-3" "CALC=\$(SUPPORT)/calc-R3-7-4"
         "DEVIOCSTATS=\$(SUPPORT)/iocStats-3-1-16" "SSCAN=\$(SUPPORT)/sscan-R2-11-5" "AREA_DETECTOR=\$(SUPPORT)/areaDetector-R3-11"
         "ADCORE=\$(AREA_DETECTOR)/ADCore" "ADSUPPORT=\$(AREA_DETECTOR)/ADSupport" "SNCSEQ=\$(SUPPORT)/seq-2-2-9")

###################################################################################################################
sed -i 's/IPAC=$(SUPPORT)\/ipac-2-15/# IPAC=$(SUPPORT)\/ipac-2-15/' $file
sed -i 's/SNCSEQ=$(SUPPORT)\/seq-2-2-5/# SNCSEQ=$(SUPPORT)\/seq-2-2-5/' $file


###################################################################################################################

os_version=$(cat /etc/redhat-release | grep -oP '(?<=release )[0-9]+' | cut -d. -f1)

if [ "$os_version" = "8" ]; then
    sed -i 's/^# \(TIRPC=YES\)/\1/' $HOME/epics/synApps/support/asyn-R4-42/configure/CONFIG_SITE
fi
cd $HOME/epics/synApps/support

make -sj
