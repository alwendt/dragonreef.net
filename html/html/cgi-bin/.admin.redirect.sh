#!/bin/sh
TERM=vt100
export TERM

IP=`/usr/bin/lynx -dump http://208.131.128.15/manager/vds_ext_ip.html | /bin/grep "\." | /bin/awk '{print $1}'`

[ -z "$IP" ] && IP="208.131.128.15"
DATE=`/bin/date '+%Z'`

GUI_IP=`/bin/gui_ip`
[ "${GUI_IP}" = "0.0.0.0" ] && GUI_IP="${IP}"

GRESELLER=`/bin/greseller`
if [ "${GRESELLER}" = "%20" ]; then
        GRESELLER=""
else
        GRESELLER="&greseller=${GRESELLER}"
fi

if [ "$1" = "vuser" ]; then
        echo "Location: https://${GUI_IP}/php/login/login_screen.php?vds_ip=dragonreef.net&uid=2133&tz=${DATE}&vds_server_ip=${IP}${GRESELLER}&p_eusr=true"
else
        echo "Location: https://${GUI_IP}/php/login/login_screen.php?vds_ip=dragonreef.net&uid=2133&tz=${DATE}&vds_server_ip=${IP}${GRESELLER}"
fi

echo ""
