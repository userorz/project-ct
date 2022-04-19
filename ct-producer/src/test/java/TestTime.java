import com.ct.common.util.DateUtil;

public class TestTime {
    public static void main(String[] args) {
        String startDate = "20210101000000";
        String endDate = "20220101000000";

        long startTime = DateUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
        long endTime = DateUtil.parse(endDate, "yyyyMMddHHmmss").getTime();
        System.out.println(startTime);
        System.out.println(endTime);
    }
}
