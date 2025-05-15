<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="/">
    <html>
     <head>
      <style>
        table {
          border-collapse: collapse;
          width: 100%;
        }

        th, td {
         text-align: left;
         padding: 8px;
        }

        tr:nth-child(even){background-color: #ddf0f2}

        th {
         background-color: #04AAAA;
         color: white;
        }
      </style>
      <title>UNIT TEST RESULT</title>
      </head>
      <body>
        <h1>SUMMARY</h1>
        <table border="1">
          <tr>
            <th>Module Name</th>
            <th>Total Number Of Tests</th>
            <th>Failed Count</th>
            <th>Time Taken</th>
          </tr>
          <xsl:for-each select="testsuites/testsuite">
            <tr>
              <td><xsl:value-of select="@name"/></td>
              <td><xsl:value-of select="@tests"/></td>
              <td><xsl:value-of select="@failures"/></td>
              <td><xsl:value-of select="@time"/></td>
            </tr>
          </xsl:for-each>
        </table>
        <h2>Settings</h2>
        <table border="1">
         <tr>
           <th>Parameter Name</th>
           <th>Parameter Value</th>
         </tr>
          <xsl:for-each select="testsuites/testsuite/properties/property">
            <tr>
              <td><xsl:value-of select="@name"/></td>
              <td><xsl:value-of select="@value"/></td>
            </tr>
          </xsl:for-each>
        
        </table>
        <h2>DETAILS</h2>
        <table border="1">
          <tr>
            <th>TestName</th>
            <th>Status</th>
            <th>TimeTaken</th>
          </tr>
          <xsl:for-each select="testsuites/testsuite/testcase">
            <tr>
              <td><xsl:value-of select="@name"/></td>
              <td>
               <xsl:choose>
                 <xsl:when test="failure/@message='Failed'">
                     <a style="color:Tomato;" href="./{@name}.log">Failed</a>
                 </xsl:when>
                 <xsl:otherwise>
                     <a style="color:DodgerBlue;" href="./{@name}.log">Passed</a>
                 </xsl:otherwise>
               </xsl:choose>
              </td>
              <td><xsl:value-of select="@time"/></td>
            </tr>
          </xsl:for-each>
        </table>
      </body>
    </html>
  </xsl:template>
</xsl:stylesheet>

