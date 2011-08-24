#!/bin/sh

# touchpad.sh    Set the touchpad enabled to on or off.

#

# Version:    @(#)touchpad.sh  0.03  19/07/2009  badsol125@gmail.com

touchpad_on(){

  /usr/bin/gconftool --set --type bool /desktop/gnome/peripherals/mouse/touchpad_enabled  true

  if test $? -eq 0;then

    echo '启用成功'

  else

    echo '启用失败'

  fi

}

touchpad_off(){

  /usr/bin/gconftool --set --type bool /desktop/gnome/peripherals/mouse/touchpad_enabled  false

  if test $? -eq 0;then

    echo '禁用成功'

  else

    echo '禁用失败'

  fi

}

case "(" in
  
  on)
  
  touchpad_on
  
  ;;
  
  off)
  
  touchpad_off
  
  ;;
  
  *)
  
  if `/usr/bin/gconftool --get /desktop/gnome/peripherals/mouse/touchpad_enabled`;then
    
    touchpad_off
    
  else
    
    touchpad_on
    
  fi
  
  ;;
  
esac

exit 0
