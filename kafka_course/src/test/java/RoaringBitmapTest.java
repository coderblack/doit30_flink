import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.roaringbitmap.RoaringBitmap;

import java.nio.charset.Charset;

public class RoaringBitmapTest {

    public static void main(String[] args) {


        RoaringBitmap bitmap1 = RoaringBitmap.bitmapOf(1, 3, 5);
        // 添加元素
        bitmap1.add(8);

        // 输出bitmap中的1的个数（元素个数）
        System.out.println(bitmap1.getCardinality());

        // 判断一个元素是否已存在
        bitmap1.contains(5);  // true



        RoaringBitmap bitmap2 = RoaringBitmap.bitmapOf();
        // 添加元素
        bitmap2.add(1);
        bitmap2.add(2);
        bitmap2.add(6);
        bitmap2.add(3);


        // 两个bitmap进行或运算
        // bitmap1.or(bitmap2);
        // System.out.println(bitmap1.getCardinality());   // 6

        // 两个bitmap进行与运算
        bitmap1.and(bitmap2);
        System.out.println(bitmap1.getCardinality());   // 2



    }


}
