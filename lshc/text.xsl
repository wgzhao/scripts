<?xml version='1.0'?>
<!--
@since Dec,2008
@Author $Author: wgzhao $
@Lastchage $Date: 2009-11-29 23:02:52 +0800 (日, 2009-11-29) $
@version $Id: text.xsl 7 2009-11-29 15:02:52Z wgzhao $
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version='1.0'>
 
<xsl:output method="text" indent="no"/>
 
<xsl:template match="array">
  <xsl:text>[</xsl:text>
  <xsl:for-each select="data/value">
    <xsl:apply-templates/>
    <xsl:if test="following-sibling::value">
      <xsl:text>, </xsl:text>
    </xsl:if>
  </xsl:for-each>
  <xsl:text>]&#10;</xsl:text>
</xsl:template>
 
<xsl:template match="string">
  <xsl:value-of select="."/>
</xsl:template>
 
<xsl:template match="member">
  <xsl:value-of select="name"/>
  <xsl:text>: </xsl:text>
  <xsl:apply-templates select="value"/>
  <xsl:text>&#10;</xsl:text>
</xsl:template>
 
<xsl:template match="/">
  <xsl:apply-templates select=".//member"/>
</xsl:template>
 
</xsl:stylesheet>
