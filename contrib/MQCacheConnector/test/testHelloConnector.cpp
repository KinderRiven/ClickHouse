#include <HelloConnector.h>
#include <memory>

int main(int argc, char **argv)
{
    auto hello = std::make_shared<HelloConnector>();
    hello->echoHello();
    return 0;
}