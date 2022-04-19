package com.ct.analysis;

import com.ct.analysis.tool.AnalysisBeanTool;
import com.ct.analysis.tool.AnalysisTextTool;
import org.apache.hadoop.util.ToolRunner;

public class AnalysisData {
    public static void main(String[] args) {
        try {
//            int result = ToolRunner.run(new AnalysisTextTool(), args);
            int result = ToolRunner.run(new AnalysisBeanTool(), args);
            System.out.println("result : " + result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
