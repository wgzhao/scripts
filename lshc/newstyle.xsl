<?xml version="1.0" encoding="utf-8"?>
<!--
@since Dec,2008
$Author: wgzhao $
@Lastchage $Date: 2010-01-15 13:42:16 +0800 (五, 2010-01-15) $
@version $Id: newstyle.xsl 12 2010-01-15 05:42:16Z wgzhao $
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:template match="/">
<html>
<head>
<meta http-equiv="content-type" content="text/html; charset=utf-8" />
<title>服务器巡检结果表</title>
<meta name="author" content="wgzhao" />
<style>
    body{
    font-size:10pt;
    font-family:San Serif；
    }
    table
    {
    border:0;
    border-collapse:collapse;
    border-top:solid windowtext 1.5pt;
    border-left:solid windowtext 1.5pt;
    border-bottom:solid windowtext 1.5pt;
    border-right:solid windowtext 1.5pt;
    margin-left:10pt;
    width:600px;
    }
    td{
    padding:2px;
    border:1px solid black;
    width:90px;
    word-break:wrapper;
    }
    th{
    background-color:gray;
    color:white;
    height:20px;
    margin-top:1px;
    padding-top:2px;
    text-align:center;
    }
    .subtitle{
    text-align:center;
    text-weight:bold;
    line-weight:110%;
    }
</style>
</head>
<body>
<table>
    <tbody>
        <tr>
            <th colspan="6">{projectname}项目 服务器巡检明细表</th>
            </tr>
<xsl:for-each  select="//server">
        <tr>
            <td colspan="6">服务器<xsl:value-of select="position()"></xsl:value-of></td>
            </tr>
        <tr>
            <td colspan="6" class="subtitle">操作系统状态</td>
            </tr>
        <tr>
            <td>主机名</td>
            <td><xsl:value-of select="hostname"></xsl:value-of></td>
            <td>ip地址</td>
            <td><pre><xsl:value-of select="ip"></xsl:value-of></pre></td>
            <td>系统时间</td>
            <td><xsl:value-of select="sysdate"></xsl:value-of></td>
        </tr>
        <tr>
            <td>系统版本</td>
            <td><xsl:value-of select="version"></xsl:value-of></td>
            <td>产品有效性</td>
            <td><xsl:value-of select="license"></xsl:value-of></td>
            <td>内核版本</td>
            <td><xsl:value-of select="kernver"></xsl:value-of></td>
        </tr>
        <tr>
            <td>进程数量</td>
            <td><xsl:value-of select="procnum"></xsl:value-of></td>
            <td>打开文件数</td>
            <td><xsl:value-of select="filenum"></xsl:value-of></td>
            <td>用户登录数</td>
            <td><xsl:value-of select="loginnum"></xsl:value-of></td>
        </tr>
        <tr>
            <td colspan="6">内存使用情况</td>
            </tr>
        <tr>
            <td colspan="6">
            <pre>
            <xsl:value-of select="memload"></xsl:value-of>
            </pre>
            </td>
            </tr>
            <xsl:for-each select="booterror|syserror|usererror">
            <tr>
            <td colspan="6"><xsl:value-of select="@name" />（为空表示没有）</td>
            </tr>
            <tr>
            <td colspan="6">
            <pre>
            <xsl:value-of select="node()" />
            </pre>
            </td>
            </tr>
            <xsl:if test = "string-length(normalize-space()) > 0">
            <tr>
            <td colspan="6">建议</td>
            </tr>
            <tr>
            <td colspan="6">
            <font color='red'>**这里替换成你的建议**</font>
            </td>
            </tr>
            </xsl:if>
            </xsl:for-each>


        <!-- HA Cluster exists or not ? -->
        <tr>
            <td class="subtitle" colspan="6">HA Cluster状态</td>
            </tr>
        <tr>
        <td colspan="6">HA Cluster 状态</td>
        </tr>
        <tr><td colspan="6"><pre><xsl:value-of select="hastat"></xsl:value-of></pre></td>
        </tr>
        <!--<tr>
            <td>HA功能测试</td>
            <td colspan="3"><xsl:value-of select="hafunc"></xsl:value-of></td>
            <td>灾难恢复测试</td>
            <td colspan="2"><xsl:value-of select="failover"></xsl:value-of></td>
        </tr>
        <tr>
            <td>应用起停测试</td>
            <td colspan="3"><xsl:value-of select="appexec"></xsl:value-of></td>
            <td>应用切换测试</td>
            <td colspan="2"><xsl:value-of select="appsw"></xsl:value-of></td>
        </tr>-->
        <!--<tr>
        <td colspan="6">HA Cluster 有效性</td>
        </tr>
        <tr><td colspan="6"><pre><xsl:value-of select="clplic"></xsl:value-of></pre></td>
        </tr>
        <tr>
        <td colspan="6">HA Cluster 状态</td>
        </tr>
        <tr><td colspan="6"><pre><xsl:value-of select="clpstat"></xsl:value-of></pre></td>
        </tr>-->

        <xsl:if test="position() != last() ">
        <!--draw double line -->
        <tr><td height="1px" colspan="6"></td></tr>
        </xsl:if>

        <!-- test new feature -->
        <!-- end new feature testing -->


</xsl:for-each>
    </tbody>
</table>
<!-- ************************************************************************** -->
</body>

</html>
</xsl:template>
</xsl:stylesheet>
