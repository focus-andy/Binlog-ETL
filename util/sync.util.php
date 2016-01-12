<?php
function array2map( $arrInput ){
    $outputMap = "" ;
    foreach( $arrInput as $k => $v ){
		if( is_array($v) ){
			$tmpStr = "" ;
			foreach( $v as $sk => $sv ){
				$tmpStr .= $sk.'['.$sv.'] ' ;
			} 
			$v = trim( $tmpStr ) ;
		}
        $v = mb_convert_encoding( $v, "UTF-8", "GBK" ) ;
        if( $outputMap == "" )
            $outputMap = "$k:$v" ;
        else
            $outputMap .= ",$k:$v" ;
    } 

    return $outputMap ;
}
function str2Hex( $string )   
{   
    $hex = "" ;   
    for( $i=0; $i < strlen($string); $i++ ) {

        if( $i < strlen($string)-3 && $string[$i] == "\\" && $string[$i+1] == 'x' 
                && ( ($string[$i+2] >= "0" && $string[$i+2] <= "1") ) 
                && ( ($string[$i+3] >= "0" && $string[$i+3] <= "9")
                    ||($string[$i+3] >= "a" && $string[$i+3] <= "f") )
          )
        {
            $hex .= $string[$i+2] ;
            $hex .= $string[$i+3] ;
            $i+=3 ;
            continue ;
        }
        $hex .= dechex(ord($string[$i])) ;
    } 
    $hex = strtoupper( $hex ) ;
    return $hex;   
}

function load_hook( $db_tbl_name ){
	$hook_dir = $GLOBALS['sync_config']['hook_dir'] ;
	$file_name = "sync.hook.". $db_tbl_name. ".php" ; 
	if( file_exists( $hook_dir.$file_name ) ){
		require_once( $hook_dir.$file_name ) ;
		return 1 ;
	}
	else{
		return 0 ;
	}
}

