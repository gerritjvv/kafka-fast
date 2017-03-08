#!/usr/bin/env bash
###
### These:
###   client


function install_lein {

    if [ ! /usr/bin/lein ]; then

        echo "installing leiningen"
        wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
        chmod a+x ./lein

        mv ./lein /usr/bin/lein

        LEIN_ROOT=true lein

        echo "Installed"
    fi
}


install_lein