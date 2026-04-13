#! /bin/tcsh -f
#
#	download hyper-compressed test data if its not already here
#
# requires:
# wget or curl
# gcc
#
# get latest version of this script from:
# wget https://bl831.als.lbl.gov/~jamesh/benchmarks/testdata/get_test_data.com
# chmod a+x get_test_data.com
# ./get_test_data.com
#

set dlurl = https://bl831.als.lbl.gov/~jamesh/benchmarks/testdata/
#set dlurl = http://smb.slac.stanford.edu/~holton/benchmarks/testdata/

# probe machine for defaults
set uname = `uname`

if("$uname" == "Linux") then
    set CPUs = `awk '/^processor/' /proc/cpuinfo | wc -l`
    echo "found $CPUs CPUs"

    set freeCPUs = `w | cat - /proc/cpuinfo - | awk '/^processor/{++p} /load aver/{l=$(NF-2)+0} END{print int(p-l+0.5)}'`
    echo "found $freeCPUs free CPUs"
endif
if("$uname" == "Darwin") then
    # for some reason on macs: don't have wget
    alias wget 'curl -o `basename \!:1` \!:1'

    set CPUs = `sysctl hw.logicalcpu | awk '/^hw./{print $NF}'`
    echo "found $CPUs CPUs"

    set freeCPUs = `w | awk -v p=$CPUs '/load aver/{l=$(NF-2)+0} END{print int(p-l+0.5)}'`
    echo "found $freeCPUs free CPUs"
endif
if(! $?CPUs) then
    ehco "WARNING: unknown platform! "
    set CPUs = 2
endif
echo ""


if("$freeCPUs" != "$CPUs") then
    echo "WARNING: machine seems busy (${freeCPUs}/${CPUs} CPUs free)."
endif

# allow user override of parameters
foreach arg ( $* )
    if("$arg" =~ CPUs=*) then
        set freeCPUs = `echo $arg | awk -F "=" '{print $2+0}'`
    endif
end


set test = `df -k . | tail -n 1 | awk 'NF>4{print ( $(NF-2)/1024/1024 > 5 )}'`
if("$test" != "1" && ! -e ./data/core_360.cbf) then
    set BAD = "need at least 5 GB of local free space for test data in `pwd`"
    goto exit
endif


set images = `ls -l data/core_*.cbf |& awk '$5>6000000' |& wc -l`
if($images != 360) then
    echo "need to generate test data."
    set starttime = `date +%s`
    mkdir -p data/ float/ logs/
    foreach prog ( noisify cbf2int int2cbf float_add )
        if(! -x ./${prog}) then
            if(! -e ${prog}.c) then
                echo "getting: ${prog}.c"
                wget ${dlurl}/${prog}.c >& /dev/null
            endif
            echo "compiling: $prog"
            gcc -O -o ${prog} ${prog}.c -lm
       endif
       if(! -x ./${prog}) then
            set BAD = "unable to obtain $prog for generating test data."
            goto exit
        endif
    end
    if(! -e noiseless/noisifyme_360.cbf) then
        if(! -e noiseless_cbfs.tar.bz2) then
            
            # for some reason on macs: aliases don't work on same line as if statements
            wget ${dlurl}/noiseless_cbfs.tar.bz2
        endif
        echo "decompressing noiseless data..."
        pbzip2 -d -c noiseless_cbfs.tar.bz2 | tar xf -
        if(! -e noiseless/noisifyme_360.cbf) then
            echo "slowly decompressing noiseless data..."
            bunzip2 -c noiseless_cbfs.tar.bz2 | tar xf noiseless_cbfs.tar.bz2
        endif
    endif
    ./cbf2int -nocheck -nostat -float noiseless/background.cbf -outfile float/background.bin >! logs/bg1.log

    if(! -s float/background.bin) then
        set BAD = "cbf2int failed, no compiler? "
        goto exit
    endif

    cat << EOF | awk '{print $0 "\r"}' >! headerstub.txt
###CBF: VERSION 1.5, CBFlib v0.7.8 - PILATUS detectors

data_

_array_data.header_convention "PILATUS_1.2"
_array_data.header_contents
;
# Detector: PILATUS3 6M, S/N 60-0000
# 1970-01-01T00:00:00.000
# Pixel_size 172e-6 m x 172e-6 m
# Silicon sensor, thickness 0.000001 m
# Exposure_time 1.0000000 s
# Exposure_period 1.0000000 s
# Tau = 0 s
# Count_cutoff 1049990 counts
# N_excluded_pixels = 0
# Wavelength 1.00000 A
# Detector_distance 0.40000 m
# Beam_xy (1264.00, 1232.00) pixels
# Start_angle x deg.
# Angle_increment 1.0000 deg.
# Phi 0.0000 deg.
;

_array_data.data
;
EOF

    cat << EOF >! job.com
#! /bin/tcsh -f
#
set num = "\$1"

./cbf2int -nocheck -nostat -float noiseless/noisifyme_\${num}.cbf -outfile float/temp_\${num}.bin
./float_add float/background.bin float/temp_\${num}.bin -outfile float/total_\${num}.bin
./noisify -floatfile float/total_\${num}.bin -nopgm \\
            -adc 0 \\
            -detpixels_x 2463 -detpixels_y 2527 -pixel 0.172 -distance 400 \\
            -phi \$num -osc 1 -scale 1 -seed \$num \\
            -noisefile data/test_\${num}.img
echo \$num |\\
cat - headerstub.txt |\\
awk 'NR==1{phi=\$1;next}\\
  /Start_angle/{\$3=sprintf("%.4f",phi)}\\
  {print}' |\\
cat >! temp_\${num}_headerstub.txt
set headersize = \`cat temp_\${num}_headerstub.txt | wc -c\`
./int2cbf -header 512 -bits 16 -unsigned -outheader \$headersize -detpixels_x 2463 -detpixels_y 2527 \\
   data/test_\${num}.img data/core_\${num}.cbf
dd conv=notrunc status=none bs=\$headersize count=1 if=temp_\${num}_headerstub.txt of=data/core_\${num}.cbf
if(\$status) then
    dd conv=notrunc bs=\$headersize count=1 if=temp_\${num}_headerstub.txt of=data/core_\${num}.cbf
endif
if(\$?DEBUG) then
    md5sum noiseless/noisifyme_\${num}.cbf
    md5sum float/total_\${num}.bin float/temp_\${num}.bin
    md5sum data/test_\${num}.img
    md5sum temp_\${num}_headerstub.txt
    md5sum data/core_\${num}.cbf
    exit
endif
rm -f temp_\${num}_headerstub.txt
rm -f data/test_\${num}.img
rm -f intimage.img
rm -f float/total_\${num}.bin float/temp_\${num}.bin
if(-s data/core_\${num}.cbf) rm -f noiseless/noisifyme_\${num}.cbf
EOF
    chmod a+x ./job.com
    set jobs = 0
    echo "adding noise to noiseless images..."
    foreach num ( `awk 'BEGIN{for(i=1;i<=360;++i) printf("%03d\n",i)}'` )
        if(-s data/core_${num}.cbf) continue
        ( ./job.com $num >&! logs/job_${num}.log  & ) >& /dev/null
        @ jobs = ( $jobs + 1 )
        while( $jobs >= $freeCPUs ) 
            set jobs = `ps -fu $USER | grep "job.com" | grep -v grep | grep -v logs | wc -l`
            echo "making data/core_${num}.cbf : $jobs jobs running..."
            sleep 5
        end
        if(-s data/core_001.cbf && ! $?MD5CHECKED) then
#            set test = `md5sum data/core_001.cbf | awk '{print ($1 == "2475a87f811e360f383ef53fe6887235")}'`
            set test = `md5sum data/core_001.cbf | awk '{print ($1 == "6dc7547f81cf7ada0b69db3e91cd0792")}'`
            if("$test" != "1") set test = `sum data/core_001.cbf | awk '{print ($1 == "29901" || $1 == "36476" || $1 == "28925")}'`
            if("$test" != "1") set test = `sum data/core_001.cbf | awk '$1 == 29901' | wc -l`
            if("$test" != "1") then
                set BAD = "data/core_001.cbf does not match expected MD5 sum. corrupted?"
                md5sum data/core_001.cbf
                echo "6dc7547f81cf7ada0b69db3e91cd0792 <- should be"
                sum data/core_001.cbf
                echo "29901 <- should be"
                echo "please report this to JMHolton@lbl.gov , thank you! "
                echo "or set environment variable MD5CHECKED to skip this check." 
                goto exit
            endif
            echo "good! checksum passed."
            set MD5CHECKED
        endif
    end
    while( $jobs > 0 ) 
        set jobs = `ps -fu $USER | grep "job.com" | grep -v grep | grep -v logs | wc -l`
        echo "making data/core_${num}.cbf : $jobs jobs still running..."
        sleep 5
    end
    set deltaT = `date +%s | awk -v t0=$starttime '{print $1-t0}'`
    echo "$deltaT s to generate test images"
endif

# for some reason: files aren't always there?
set retries = 30
while("$images" != "360" && $retries > 0)
    @ retries = ( $retries - 1 )
    wait
    sync
    ls -l data/ >& /dev/null
    sleep 1
    set images = `ls -l data/core_*.cbf | awk '$5>6000000' |& wc -l`
    if("$images" != "360") echo "waiting for ${images} images to appear..."
end
if($images != 360) then
    set BAD = "cannot generate complete test data, only $images generated."
    goto exit
endif
if(! $?DEBUG) then
    # should be essentially empty
    rm -rf float/
    rm -rf noiseless/
    rm -rf logs/
    rm -f headerstub.txt
endif

set test = `md5sum data/core_001.cbf | awk '{print ($1 == "6dc7547f81cf7ada0b69db3e91cd0792")}'`
if("$test" == "") set test = `sum data/core_001.cbf | awk '{print ($1 == "29901" || $1 == "36476" || $1 == "28925")}'`
if("$test" != "1") set test = `sum data/core_001.cbf | awk '$1 == 29901' | wc -l`
if("$test" != "1" && ! $?MD5CHECKED) then
    set BAD = "data/core_001.cbf does not match expected MD5 sum. corrupted?"
    goto exit
endif

if(! -e data/test_03600.cbf) then
    set starttime = `date +%s`
    echo "expanding data..."
    foreach num ( `awk 'BEGIN{for(i=1;i<=3600;++i) printf("%05d\n",i)}'` )
        set cnum = `echo $num | awk '{printf("%03d",($1-1)%360+1)}'`
        if(! -e data/test_${num}.cbf) then
            echo "$num" | awk '{printf("%s\r",$1)}'
            ln -sf core_${cnum}.cbf data/test_${num}.cbf
        endif
    end
    echo ""
    set deltaT = `date +%s | awk -v t0=$starttime '{print $1-t0}'`
    echo "$deltaT s to expand 360-image core to 3600 test images"
endif

if (! -x ./log_timestamp.tcl) then
cat << EOF >! log_timestamp.tcl
#! /bin/sh
# use tclsh in the path \
exec tclsh "\$0" "\$@"
#
#       encode a logfile stream with time stamps
#
#
#
set start [expr [clock clicks -milliseconds]/1000.0]

while { ! [eof stdin] } {
    set line "[gets stdin]"
    puts "[clock format [clock seconds] -format "%a %b %d %T %Z %Y"] [clock seconds] [format "%12.3f" [expr [clock clicks -milliseconds]/1000.0 - \$start]] \$line"

}
EOF
chmod a+x log_timestamp.tcl
endif

exit:

if($?BAD) then
    echo "ERROR: $BAD"
    exit 9
endif

exit


##################################3
# notes:
# run this on local machine:

wget http://smb.slac.stanford.edu/~holton/benchmarks/testdata/get_test_data.com
chmod a+x get_test_data.com
./get_test_data.com


