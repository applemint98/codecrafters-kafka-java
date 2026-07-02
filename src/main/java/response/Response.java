package response;

import java.io.DataOutputStream;
import java.io.IOException;

public interface Response {

    void writeTo(DataOutputStream out) throws IOException;
}
