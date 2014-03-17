scan 'sn_table', {STARTROW => '20140204', ENDROW => '20140205'}
scan 'sn_table', { COLUMNS => 'a:day', LIMIT => 10, FILTER => "ValueFilter( =, 'binaryprefix:20140204' )" }

