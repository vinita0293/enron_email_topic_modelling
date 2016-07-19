/**
 * Created by vinita on 7/7/16.
 */

import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class codechef {

    /* Name of the class has to be "Main" only if the class is public. */

        public static void main (String[] args) throws java.lang.Exception
        {
            BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
            PrintWriter pw=new PrintWriter(System.out);

            int testcase=Integer.parseInt(br.readLine());


            while(testcase>0)
            {
                testcase--;
              int n=Integer.parseInt(br.readLine());
                int a=1;
                int  minus=n-1;

                if((int)Math.sqrt(n)*(int)Math.sqrt(n)==n)

                    pw.println(0);

                else {
                    int count=0;
                    for (int i = 2; i < (n / 2); i++) {

                        if (n % i == 0) {
                           count++;
                            a = a * i;
                            n = n / i;
                            i = 1;
                            pw.println(a+" "+n);
                            if (Math.abs(a - n) < minus) {

                                minus = Math.abs(a - n);
                                pw.println(minus);
                            }
                        }
                        /*if (n < a)
                            break;
*/                    }

                    pw.println("count ="+count +"  "+minus);
                }
            }

            pw.flush();
            pw.close();
        }
    }




