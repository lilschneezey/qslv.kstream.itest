package qslv.kstream.itest;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
@SelectPackages( "qslv.kstream.itest")
@IncludeClassNamePatterns("^(Itest_.*|.+[.$]Itest_.*)$")
class UnitSuiteKStreamIntegration {


}
